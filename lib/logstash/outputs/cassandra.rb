# encoding: utf-8
require "logstash/outputs/base"
require "logstash/namespace"

# An example output that does nothing.
class LogStash::Outputs::Cassandra < LogStash::Outputs::Base
  config_name "cassandra"

  # A hash of options for establishing cluster connection via DataStax Ruby Driver
  # See: http://datastax.github.io/ruby-driver/api/#cluster-class_method
  config :options, :validate => :hash, :default => {}

  # Strings specifying keyspace and table name to use for events
  config :keyspace, :validate => :string, :default => "logstash"
  config :table, :validate => :string, :default => "logstash"

  public
  def register
      require 'cassandra'
      require 'time'
      cluster = Cassandra.cluster(@options)
      @session = cluster.connect
      @session.execute("create keyspace if not exists #{@keyspace} with replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};")
      @session.execute("use #{@keyspace};")
      @session.execute("create table if not exists #{@table} (
                        id uuid primary key,
                        version text,
                        \"timestamp\" timestamp,
                        tags set<text>,
                        type text,
                        message text,
                        path text,
                        host text,
                        b_ map<text, boolean>,
                        d_ map<text, timestamp>,
                        f_ map<text, float>,
                        i_ map<text, bigint>,
                        s_ map<text, text>);")
      cluster.refresh_schema
      @columns = cluster.keyspace(@keyspace).table(@table).columns.map {|col| col.name}
      cql = "insert into #{@table} (#{@columns.join(",")}) values (#{@columns.map {|col| ":" + col}.join(",")})"
      @statement = @session.prepare(cql)
      @generator = Cassandra::Uuid::Generator.new
  end # def register

  public
  def receive(event)
      return unless output?(event)
      generic = {'b_' => {}, 'd_' => {}, 'i_' => {}, 'f_' => {}, 's_' => {}}
      args = event.to_hash
      args["id"] = @generator.uuid
      args["version"] = args.delete("@version")
      args["timestamp"] = Time.iso8601(args.delete("@timestamp").iso8601)
      args.each do |key, value|
          if not @columns.include?(key)
              if value.is_a?(TrueClass) or value.is_a?(FalseClass)
                  prefix = 'b_'
              elsif value.is_a?(Time)
                  prefix = 'd_'
              elsif value.is_a?(Fixnum)
                  prefix = 'i_'
              elsif value.is_a?(Float)
                  prefix = 'f_'
              elsif value.is_a?(String)
                  prefix = 's_'
              else
                  prefix = 's_'
                  value = JSON.generate(value)
              end
              if not value.nil?
                  generic[prefix][prefix + key] = value
              end
              args.delete(key)
          end
      end
      generic.each do |key, value|
          if @columns.include?(key)
              args[key] = value
          end
      end
      @columns.each do |key, value|
          if not args.has_key?(key)
              args[key] = nil
          end
      end
      @session.execute(@statement, arguments: args)
  end # def event
end # class LogStash::Outputs::Cassandra
