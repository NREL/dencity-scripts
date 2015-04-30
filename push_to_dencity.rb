require 'faraday'
require 'logger'
require 'multi_json'
require 'parallel'
require 'pp'
require 'colored'

@logger = Logger.new 'dencity.log'
#@hostname = 'http://dencity.org'
#@hostname = 'http://localhost:3000'
@hostname = 'http://docker.dencity.org'

building_lookup = {
    apt_highrise: {
        building_type: "Highrise Apartment",
        principal_hvac: "Air Source Heat Pump"
    },
    apt_midrise: {
        building_type: "Midrise Apartment",
        principal_hvac: "Packaged DX"
    },
    assisted_living: {
        building_type: "Assisted Living",
        principal_hvac: "Packaged DX"
    },
    city_hall: {
        building_type: "City Hall",
        principal_hvac: "Packaged DX"
    },
    community_center: {
        building_type: "Community Center",
        principal_hvac: "Packaged DX"
    },
    courthouse: {
        building_type: "Courthouse",
        principal_hvac: "Packaged DX"
    },
    large_hotel: {
        building_type: "Large Hotel",
        principal_hvac: "Central"
    },
    large_office: {
        building_type: "Large Office",
        principal_hvac: "Central"
    },
    library: {
        building_type: "Library",
        principal_hvac: "Air Source Heat Pump"
    },
    medical_office: {
        building_type: "Medical Office",
        principal_hvac: "Terminal DX"
    },
    medium_office: {
        building_type: "Medium Office",
        principal_hvac: "Packaged DX"
    },
    police_station: {
        building_type: "Police Station",
        principal_hvac: "Air Source Heat Pump"
    },
    post_office: {
        building_type: "Post Office",
        principal_hvac: "Unknown"
    },
    primary_school: {
        building_type: "Primary School",
        principal_hvac: "Packaged DX"
    },
    religious_building: {
        building_type: "Religious Building",
        principal_hvac: "Unknown"
    },
    retail: {
        building_type: "Retail",
        principal_hvac: "Packaged DX"
    },
    retail_stripmall: {
        building_type: "Retail (Strip Mall)",
        principal_hvac: "Packaged DX"
    },
    secondary_school: {
        building_type: "Secondary School",
        principal_hvac: "Central"
    },
    senior_center: {
        building_type: "Senior Center",
        principal_hvac: "Packaged DX"
    },
    small_hotel: {
        building_type: "Small Hotel",
        principal_hvac: "Terminal DX"
    },
    small_office: {
        building_type: "Small Office",
        principal_hvac: "Unknown"
    },
    warehouse: {
        building_type: "Warehouse",
        principal_hvac: "Packaged DX"
    },
    warehouse_heat_only: {
        building_type: "Warehouse (Heat Only)",
        principal_hvac: "Packaged DX"
    }
}

# Get the root directory where all the buildings are defined
buildings = Dir.glob('../assetscore-results-data/distributions_20140915/*')

begin
  buildings.each do |building|
#Parallel.each(buildings, in_processes: 4) do |building|
    next unless File.directory? building
    building_name = File.basename(building)
    puts building_name
    building_type = building_lookup[building_name.to_sym][:building_type]
    primary_hvac = building_lookup[building_name.to_sym][:principal_hvac]

    upload_cache = File.exist?("#{building}_upload.log") ? MultiJson.load(File.read("#{building}_upload.log"), symbolize_names: true) : {uploaded: []}

    puts "Processing #{building_name} with type #{building_type}".green

    # get the metadata file
    analysis_json = Dir["#{building}/analysis_*_dencity.json"].first

    c = Faraday.new(url: @hostname) do |faraday|
      faraday.request :url_encoded # form-encode POST params
      faraday.use Faraday::Response::Logger, @logger
      # faraday.response @logger # log requests to STDOUT
      faraday.adapter Faraday.default_adapter # make requests with Net::HTTP
    end
    c.basic_auth('nicholas.long@nrel.gov', 'testing123')

    r = c.post do |req|
      req.url 'api/analysis'
      req.headers['Content-Type'] = 'application/json'
      req.body = File.read(analysis_json)
    end
    #puts r.status
    #puts r.body

    prov_id = MultiJson.load(r.body, symbolize_keys: true)[:provenance][:id]
    #post the analysis_json to get the prov id
    puts "  Provenance ID from the server is #{prov_id}"

    # get the building jsons
    jsons = Dir["#{building}/data_points/*_dencity.json"] #.first(32)

    # Take the delta of the two to find the actual JSONS to upload
    jsons = jsons - upload_cache[:uploaded]

    #jsons.each do |d|
    begin
      Parallel.each(jsons, in_threads: 2) do |d|
        start_time = Time.now
        #jsons.each do |d|
        # Create a connection for every thread -- this is hitting the api pretty hard tho... hmm
        c ||= Faraday.new(url: @hostname) do |faraday|
          faraday.request :url_encoded # form-encode POST params
          faraday.use Faraday::Response::Logger, @logger
          # faraday.response @logger # log requests to STDOUT
          faraday.adapter Faraday.default_adapter # make requests with Net::HTTP
          #builder.request  :basic_authentication, 'nicholas.long@nrel.gov', 'testing123'
        end
        c.basic_auth('nicholas.long@nrel.gov', 'testing123')

        r = c.post do |req|
          req.url "api/structure?provenance_id=#{prov_id}"
          req.headers['Content-Type'] = 'application/json'

          # inject the building type and fix any units
          data = MultiJson.load(File.read(d))
          data['structure']['building_type'] = building_type
          # look up the principal hvac system type
          if primary_hvac.empty?
            pp data['structure']
            fail "no primary hvac defined for #{building_type}"
          end

          data['structure']['primary_hvac_system'] = primary_hvac

          data['structure'].delete('total_site_energy_intensity')
          data['structure'].delete('total_source_energy_intensity')
          data['structure'].delete('total_electricity_intensity')
          data['structure'].delete('total_natural_gas_intensity')

          data['metadata'] = {}
          # pull out the UUID from filename "data_point_ce5a1af0-1d04-0132-cdca-12313d185606_dencity"
          data['metadata']['user_defined_id'] = d.scan(/data_point_(.*)_dencity/).first[0]

          req.body = data.to_json
        end
        #puts r.status

        if r.status == 500
          puts "  Failed! #{File.basename(d)} in #{Time.now - start_time} s".red
          next
        end

        # check if there were any warnings. And if so, then crash out
        r = MultiJson.load(r.body)

        if r['warnings'].size > 0
          pp r['warnings']
          fail "Building has #{r['warnings'].size} warnings (treated as errors)"
        end

        # save to a log so that we don't try to reupload the file in the future
        upload_cache[:uploaded] << d

        puts "  Uploaded #{File.basename(d)} in #{Time.now - start_time} s"
      end
    rescue => e
      raise "#{e.message}: #{e.backtrace.join('\n')}"
    ensure
      File.open("#{building}_upload.log", 'w') { |f| f << MultiJson.dump(upload_cache, pretty: true) }
    end

  end

rescue => e
  puts "errored #{e.message}... retrying"
  sleep 10
  retry
end
