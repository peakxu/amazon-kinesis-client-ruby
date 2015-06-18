#! /usr/bin/env ruby
#
#  Copyright 2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
#  Licensed under the Amazon Software License (the "License").
#  You may not use this file except in compliance with the License.
#  A copy of the License is located at
#
#  http://aws.amazon.com/asl/
#
#  or in the "license" file accompanying this file. This file is distributed
#  on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
#  express or implied. See the License for the specific language governing
#  permissions and limitations under the License.

require 'aws-sdk'

module Aws
  module KCLrb
    # @api private
    class Producer
      def initialize(service, stream_name, shard_count = nil)
        @stream_name = stream_name
        @shard_count = shard_count
        @kinesis = service
      end

      def delete_stream_if_exists
        begin
          @kinesis.delete_stream(:stream_name => @stream_name)
          puts "Deleted stream #{@stream_name}"
        rescue Aws::Kinesis::Errors::ResourceNotFoundException
          # nothing to do
        end
      end

      def put_record(key, data)
        r = @kinesis.put_record(:stream_name => @stream_name,
                                :data => data,
                                :partition_key => key)
        puts "Put record to shard '#{r[:shard_id]}' (#{r[:sequence_number]}): '#{data}'"
      end

      def create_stream_if_not_exists
        begin
          desc = get_stream_description
          if desc[:stream_status] == 'DELETING'
            fail "Stream #{@stream_name} is being deleted. Please re-run the script."
          elsif desc[:stream_status] != 'ACTIVE'
            wait_for_stream_to_become_active
          end
          if @shard_count && desc[:shards].size != @shard_count
            fail "Stream #{@stream_name} has #{desc[:shards].size} shards, while requested number of shards is #{@shard_count}"
          end
          puts "Stream #{@stream_name} already exists with #{desc[:shards].size} shards"
        rescue Aws::Kinesis::Errors::ResourceNotFoundException
          puts "Creating stream #{@stream_name} with #{@shard_count || 2} shards"
          @kinesis.create_stream(:stream_name => @stream_name,
                                 :shard_count => @shard_count || 2)
          wait_for_stream_to_become_active
        end
      end

      private

      def get_stream_description
        r = @kinesis.describe_stream(:stream_name => @stream_name)
        r[:stream_description]
      end

      def wait_for_stream_to_become_active
        sleep_time_seconds = 3
        status = get_stream_description[:stream_status]
        while status && status != 'ACTIVE' do
          puts "#{@stream_name} has status: #{status}, sleeping for #{sleep_time_seconds} seconds"
          sleep(sleep_time_seconds)
          status = get_stream_description[:stream_status]
        end
      end
    end
  end
end
