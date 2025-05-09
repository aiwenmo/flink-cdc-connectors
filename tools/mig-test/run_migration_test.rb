#!/usr/bin/env ruby
# frozen_string_literal: true

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

require 'pathname'
require 'securerandom'

WAITING_SECONDS = 20
FLINK_HOME = ENV['FLINK_HOME']
throw 'Unspecified `FLINK_HOME` environment variable.' if FLINK_HOME.nil?
FLINK_HOME = Pathname.new(FLINK_HOME).realpath

SOURCE_PORT = 3306
DATABASE_NAME = 'fallen'
TABLES = ['girl'].freeze

def exec_sql_source(sql)
  `mysql -h 127.0.0.1 -P#{SOURCE_PORT} -uroot --skip-password -e "USE #{DATABASE_NAME}; #{sql}"`
end

def put_mystery_data(mystery)
  exec_sql_source("REPLACE INTO girl(id, name) VALUES (17, '#{mystery}');")
end

def ensure_mystery_data(mystery)
  raise StandardError, 'Failed to get specific mystery string' unless `cat #{FLINK_HOME}/log/*.out`.include? mystery
end

def extract_job_id(output)
  current_job_id = output.split("\n").filter { _1.start_with?('Job ID: ') }.first&.split&.last
  raise StandardError, "Failed to submit Flink job. Output: #{output}" unless current_job_id&.length == 32
  current_job_id
end

puts '   Waiting for source to start up...'
next until exec_sql_source("SELECT '1';") == "1\n1\n"

def test_migration_chore(from_version, to_version)
  TABLES.each do |table_name|
    exec_sql_source("DROP TABLE IF EXISTS #{table_name};")
    exec_sql_source("CREATE TABLE #{table_name} (ID INT NOT NULL, NAME VARCHAR(17), PRIMARY KEY (ID));")
  end

  # Clear previous savepoints and logs
  `rm -rf savepoints`

  # Prepare for current YAML file
  yaml_job_template_file = 'conf/pipeline-route.yaml'
  yaml_job_file = 'conf/temp.yaml'
  yaml_content = File.open(yaml_job_template_file).read.gsub('${PIPELINE_NAME}',
                                                             "Pipeline Migration Job (#{from_version} -> #{to_version})")
  File.write(yaml_job_file, yaml_content)

  # Submit current pipeline job file
  submit_job_output = `bash ./cdc-versions/#{from_version}/bin/flink-cdc.sh --flink-home #{FLINK_HOME} #{yaml_job_file}`
  puts "   #{submit_job_output}"
  current_job_id = extract_job_id(submit_job_output)

  puts "   Current Job ID: #{current_job_id}"

  # Verify if data sync works
  random_string_1 = SecureRandom.hex(8)
  put_mystery_data random_string_1
  sleep WAITING_SECONDS
  ensure_mystery_data random_string_1

  # Stop current job and create a savepoint
  puts `#{FLINK_HOME}/bin/flink stop --savepointPath #{Dir.pwd}/savepoints #{current_job_id}`
  savepoint_file = `ls savepoints`.split("\n").last

  # Migrate to a newer CDC version
  puts "   Submitting CDC jobs at #{to_version}..."
  submit_job_output = `bash ./cdc-versions/#{to_version}/bin/flink-cdc.sh --from-savepoint #{Dir.pwd}/savepoints/#{savepoint_file} --allow-nonRestored-state --flink-home #{FLINK_HOME} #{yaml_job_file}`
  puts "#{submit_job_output}"
  new_job_id = extract_job_id(submit_job_output)

  puts "   Upgraded Job ID: #{new_job_id}"

  # Verify if data sync works
  puts "Submitted job at #{to_version} as #{new_job_id}"
  random_string_2 = SecureRandom.hex(8)
  put_mystery_data random_string_2
  sleep WAITING_SECONDS
  ensure_mystery_data random_string_2
  puts `#{FLINK_HOME}/bin/flink cancel #{new_job_id}`
  true
end

def test_migration(from_version, to_version)
  puts "➡️ [MIGRATION] Testing migration from #{from_version} to #{to_version}..."
  puts "   with Flink #{FLINK_HOME}..."
  begin
    result = test_migration_chore from_version, to_version
    if result
      puts "✅ [MIGRATION] Successfully migrated from #{from_version} to #{to_version}!"
    else
      puts "❌ [MIGRATION] Failed to migrate from #{from_version} to #{to_version}..."
    end
    result
  rescue => e
    puts "❌ [MIGRATION] Failed to migrate from #{from_version} to #{to_version}...", e
    false
  end
end

version_list = case ARGV[0]
               when '1.19.2' then %w[3.2.0 3.2.1 3.3.0 3.4-SNAPSHOT]
               when '1.20.1' then %w[3.2.1 3.3.0 3.4-SNAPSHOT]
               else throw "Unknown Flink target version: #{ARGV[0]}."
               end

@failures = []

new_version = version_list.last

version_list.each do |old_version|
  puts "-> Testing migrating from #{old_version} to latest snapshot."
  puts 'Restarting cluster...'
  `#{FLINK_HOME}/bin/stop-cluster.sh`
  puts 'Stopped cluster.'
  `#{FLINK_HOME}/bin/start-cluster.sh`
  puts 'Started cluster.'

  result = test_migration old_version, new_version
  @failures << "#{old_version} => #{new_version}" unless result
end

if @failures.any?
  puts 'Some migration to snapshot version tests failed. Details: '
  puts @failures
  abort 'Some migration to snapshot version tests failed.'
end
