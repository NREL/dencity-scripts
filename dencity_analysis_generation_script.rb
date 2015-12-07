require 'multi_json'
require 'dencity'
require 'time'

hostname = 'http://lost.in.translation:2003'
user_id = 'johnny.marco@somewhere.net'
auth_code = 'murrayChristmas'
local_analysis_uuid = ''

conn = Dencity.connect({hostname: hostname})
fail "Could not connect to DEnCity server at #{hostname}." unless conn.connected?
begin
  r = conn.login(user_id, auth_code)
rescue Faraday::ParsingError => user_id_failure
  fail "Error in user_id field: #{user_id_failure.message}"
rescue MultiJson::ParseError => authentication_failure
  fail "Error in attempted authentication: #{login_failure.message}"
end
user_uuid = r.id

# Find the analysis.json file that SHOULD BE IN THE FOLDER THAT THIS SCRIPT IS IN (or change the below)
# Check that the analysis has not yet been registered with the DEnCity instance.
# TODO This should be simplified with a retrieve_analysis_by_user_defined_id' method in the future
analysis = MultiJson.load(File.read('analysis.json'))
user_analyses = []
r = conn.dencity_get 'analyses'
runner.registerError('Unable to retrieve analyses from DEnCity server') unless r['status'] == 200
r['data'].each do |dencity_analysis|
  user_analyses << dencity_analysis['id'] if dencity_analysis['user_id'] == user_uuid
end
found_analysis_uuid = false
user_analyses.each do |analysis_id|
  analysis = conn.retrieve_analysis_by_id(analysis_id)
  if analysis['user_defined_id'] == local_analysis_uuid
    found_analysis_uuid = true
    break
  end
end
fail "Analysis with user_defined_id of #{local_analysis_uuid} found on DEnCity." if found_analysis_uuid

# Create the analysis hash to be uploaded to DEnCity.
dencity_hash = {}
a = analysis
prov_fields = %w(uuid created_at name display_name description)
provenance = a.select { |key, _| prov_fields.include? key }
provenance['user_defined_id'] = local_analysis_uuid
provenance['user_created_date'] = Time.now
provenance['analysis_types'] = a['problem']['analysis_type']
measure_metadata = []
if a['problem']

  if a['problem']['algorithm']
    provenance['analysis_information'] = a['problem']['algorithm']
  else
    fail 'No algorithm found in the analysis.json.'
  end

  if a['problem']['workflow']
    a['problem']['workflow'].each do |wf|
      new_wfi = {}
      new_wfi['id'] = wf['measure_definition_uuid']
      new_wfi['version_id'] = wf['measure_definition_version_uuid']

      # Eventually all of this could be pulled directly from BCL
      new_wfi['name'] = wf['measure_definition_class_name'] if wf['measure_definition_class_name']
      new_wfi['display_name'] = wf['measure_definition_display_name'] if wf['measure_definition_display_name']
      new_wfi['type'] = wf['measure_type'] if wf['measure_type']
      new_wfi['modeler_description'] = wf['modeler_description'] if wf['modeler_description']
      new_wfi['description'] = wf['description'] if wf['description']
      new_wfi['arguments'] = []

      if wf['arguments']
        wf['arguments'].each do |arg|
          wfi_arg = {}
          wfi_arg['display_name'] = arg['display_name'] if arg['display_name']
          wfi_arg['display_name_short'] = arg['display_name_short'] if arg['display_name_short']
          wfi_arg['name'] = arg['name'] if arg['name']
          wfi_arg['data_type'] = arg['value_type'] if arg['value_type']
          wfi_arg['default_value'] = nil
          wfi_arg['description'] = ''
          wfi_arg['display_units'] = '' # should be haystack compatible unit strings
          wfi_arg['units'] = '' # should be haystack compatible unit strings

          new_wfi['arguments'] << wfi_arg
        end
      end

      if wf['variables']
        wf['variables'].each do |arg|
          wfi_var = {}
          wfi_var['display_name'] = arg['argument']['display_name'] if arg['argument']['display_name']
          wfi_var['display_name_short'] = arg['argument']['display_name_short'] if arg['argument']['display_name_short']
          wfi_var['name'] = arg['argument']['name'] if arg['argument']['name']
          wfi_var['default_value'] = nil
          wfi_var['data_type'] = arg['argument']['value_type'] if arg['argument']['value_type']
          wfi_var['description'] = ''
          wfi_var['display_units'] = arg['units'] if arg['units']
          wfi_var['units'] = '' # should be haystack compatible unit strings
          new_wfi['arguments'] << wfi_var
        end
      end

      measure_metadata << new_wfi
    end
  else
    fail 'No workflow found in the analysis.json'
  end

  dencity_hash['provenance'] = provenance
  dencity_hash['measure_definitions'] = measure_metadata
else
  fail 'No problem found in the analysis.json'
end

# Write the analysis DEnCity hash to dencity_analysis.json
f = File.new('dencity_analysis.json', 'wb')
f.write(MultiJson.encode(dencity_hash, :pretty => true))
f.close

# Upload the processed analysis json.
analysis = conn.load_analysis 'dencity_analysis.json'
begin
  analysis_response = analysis.push
rescue StandardError => e
  runner.registerError("Upload failure: #{e.message} in #{e.backtrace.join('/n')}")
else
  print 'Successfully uploaded processed analysis json file to the DEnCity server.'
  puts analysis_response
end