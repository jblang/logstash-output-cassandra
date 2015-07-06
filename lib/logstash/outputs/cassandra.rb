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
      @columns = cluster.keyspace(@keyspace).table(@table).columns
      cql = "insert into #{@table} (#{@columns.map {|col| col.name}.join(",")}) values (#{@columns.map {|col| ":" + col.name}.join(",")})"
      @session = cluster.connect(@keyspace)
      @statement = @session.prepare(cql)
      @generator = Cassandra::Uuid::Generator.new
  end # def register

  public
  def receive(event)
      return unless output?(event)
      args = event.to_hash
      args["id"] = @generator.uuid
      args["version"] = args.delete("@version").to_i
      args["timestamp"] = Time.iso8601(args.delete("@timestamp").iso8601)
      @session.execute(@statement, arguments: args)
  end # def event
end # class LogStash::Outputs::Cassandra
