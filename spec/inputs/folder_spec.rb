# encoding: utf-8

require "logstash/devutils/rspec/spec_helper"
require "tempfile"
require "stud/temporary"

describe "inputs/folder" do

  describe "collect new files" do
    tmpfile_path = Stud::Temporary.directory

    config <<-CONFIG
      input {
        folder {
          path => "#{tmpfile_path}"
        }
      }
    CONFIG

    input do |pipeline, queue|
      Thread.new { pipeline.run }
      sleep 0.1 while !pipeline.ready?

      # at this point even if pipeline.ready? == true the plugins
      # threads might still be initializing so we cannot know when the
      # file plugin will have seen the original file, it could see it
      # after the first(s) hello world appends below, hence the
      # retry logic.
      retries = 0
      loop do
        insist { retries } < 20 # 2 secs should be plenty?

        File.open("#{tmpfile_path}/#{retries}.log", "w") do |fd|
          fd.puts("1")
        end

        if queue.size >= 1
          events = 1.times.collect { queue.pop }
          insist { events[0]["message"] } == "1\n"
          break
        end

        sleep(0.1)
        retries += 1
      end
    end
  end
end
