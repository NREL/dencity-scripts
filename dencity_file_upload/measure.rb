# Reporting measure to push structure hash to DEnCity server.

# Author: Henry Horsey (github: henryhorsey / rhorsey)
# Creation Date: 11/26/2015

require 'dencity'
require 'openstudio'

class DencityFileUpload < OpenStudio::Ruleset::ReportingUserScript

  # Define the human-readable name
  def name
    'DEnCity Datapoint Upload'
  end

  # Define the arguments that the user will input
  def arguments
    args = OpenStudio::Ruleset::OSArgumentVector.new

    # URL of the DEnCity server that will be posted to
    hostname = OpenStudio::Ruleset::OSArgument::makeStringArgument('hostname', true)
    hostname.setDisplayName('URL of the DEnCity Server')
    hostname.setDefaultValue('http://www.dencity.org')
    args << hostname

    # DEnCity server user id at hostname
    user_id = OpenStudio::Ruleset::OSArgument::makeStringArgument('user_id',true)
    user_id.setDisplayName('User ID for DEnCity Server')
    args << user_id

    # DEnCIty server user id's password
    auth_code = OpenStudio::Ruleset::OSArgument::makeStringArgument('auth_code', true)
    auth_code.setDisplayName('Authentication code for User ID on DEnCity server')
    args << user_id

    # Building type for DEnCity's metadata
    file_path = OpenStudio::Ruleset::OSArgument::makeStringArgument('file_path', true)
    file_path.setDisplayName('File Path')
    args << file_path

    # Name of the file in DEnCity
    dencity_file_name = OpenStudio::Ruleset::OSArgument::makeOptionalStringArgument('dencity_file_name', true)
    dencity_file_name.setDisplayName('DEnCity File Name')
    args << file_path

    args

  end

  def run(runner, user_arguments)
    super(runner, user_arguments)

    # Use the built-in error checking
    unless runner.validateUserArguments(arguments, user_arguments)
      false
    end

    # Unpack DEnCity hostname, user_id, and auth_code
    hostname = runner.getStringArgumentValue('hostname', user_arguments)
    user_id = runner.getStringArgumentValue('user_id', user_arguments)
    auth_code = runner.getStringArgumentValue('auth_code', user_arguments)
    file_path = runner.getStringArgumentValue('file_path', user_arguments)

    # Check connection to hostname and authenticate connection
    conn = Dencity.connect({hostname: hostname})
    runner.registerError "Could not connect to DEnCity server at #{hostname}." unless conn.connected?
    begin
      r = conn.login(user_id, auth_code)
    rescue Faraday::ParsingError => user_id_failure
      runner.registerError "Error in user_id field: #{user_id_failure.message}"
    rescue MultiJson::ParseError => authentication_failure
      runner.registerError "Error in attempted authentication: #{authentication_failure.message}"
    end

    # Check that the file exists
    runner.registerError("Could not find file #{file_path}.") unless File.exists? file_path

    # Check dencity_file_name, and if not initialized set to the file_path file name.
    runner.getOptionalStringArgumentValue('dencity_file_name', user_arguments)
      building_type.is_initialized? ? building_type = building_type.get : building_type = nil
    end

    # Find the structure id
    measure_results = runner.results
    if measure_results.keys.include? 'dencity_datapoint_upload'
      if measure_results['dencity_datapoint_upload'].keys.include? 'structure_id'
        structure_id = measure_results['dencity_datapoint_upload']['structure_id']
      else
        runner.registerError 'Could not find the `structure_id` field in the `dencity_datapoint_upload` measure results.'
      end
    else
      runner.registerError 'Could not find `dencity_datapoint_upload` measure in runner.results to retrieve the dencity structure id from.'
    end

    # Get the structure object
    structure = conn.dencity_get "structures/#{structure_id}"
    runner.registerError('Unable to retrieve structure from DEnCity server.') unless structure.status == 200

    # Upload the file
    begin
      structure_responce = structure.upload_file(file_path)
    rescue StandardError => e
      runner.registerError("Upload failure: #{e.message} in #{e.backtrace.join('/n')}")
    else
      runner.registerInfo "Successfully uploaded #{file_path} to the DEnCity server for structure #{structure_id}."
      runner.registerInfo "Push responce: #{structure_response}"
    end

    true

  end

end

DencityFileUpload.new.registerWithApplication