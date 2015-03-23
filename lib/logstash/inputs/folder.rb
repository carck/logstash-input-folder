# encoding: utf-8
require "logstash/inputs/base"
require "logstash/namespace"

require "pathname"
require "socket"
require 'thread'
require "rb-inotify"

class LogStash::Inputs::Folder < LogStash::Inputs::Base
  config_name "folder"

  default :codec, "plain"

  config :path, :validate => :string, :required => true


  public
  def register
    @logger.info("Registering folder input", :path => @path)

    if Pathname.new(@path).relative?
      raise ArgumentError.new("File paths must be absolute, relative path specified: #{path}")
    end

    @fileQueue = Queue.new
  end

  public
  def run(queue)
    hostname = Socket.gethostname

    @notifier = INotify::Notifier.new

    @notifier.watch(@path, :create) do |event|
      @logger.debug("new file", :path => event.name)
      @fileQueue << event
    end

    @consumer = Thread.new do
      while true do
          fileEvent = @fileQueue.pop
          fileName = "#{@path}/#{fileEvent.name}"
          begin
            if File.writable?(fileName)
              @codec.decode(File.read(fileName)) do |event|
                decorate(event)
                event["host"] = hostname if !event.include?("host")
                event["path"] = fileName
                queue << event
              end
            else
              sleep(1)
              @fileQueue << fileEvent
            end
          rescue Exception => e
            @fileQueue << fileEvent
          end
        end
      end

      @notifier.run

      finished

      puts "folder plugin started"
    end

    public
    def teardown
      @notifier.close
      @consumer
    end
  end
