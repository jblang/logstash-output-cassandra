# encoding: utf-8
require "logstash/outputs/base"
require "logstash/namespace"

# Output plugin for http://cassandra.apache.org/[Apache Cassandra]
# using the https://github.com/datastax/ruby-driver[DataStax Ruby Driver].
#
class LogStash::Outputs::Cassandra < LogStash::Outputs::Base
  config_name "cassandra"

  # A hash of options for establishing cluster connection. See the
  # http://datastax.github.io/ruby-driver/api/#cluster-class_method[driver API documentation]
  # for a list of supported options.
  config :options, :validate => :hash, :default => {}

  # The keyspace to use; if it doesn't exist, it will be automatically created 
  # using SimpleStrategy with a replication factor of 1.
  config :keyspace, :validate => :string, :default => "logstash"

  # The table to use; if it doesn't exist, it will be automatically created using
  # the following schema:
  # ----
  # create table if not exists logstash (
  # id uuid primary key,
  # version text,
  # \"timestamp\" timestamp,
  # tags set<text>,
  # type text,
  # message text,
  # path text,
  # host text,
  # b_ map<text, boolean>,
  # d_ map<text, timestamp>,
  # f_ map<text, float>,
  # i_ map<text, bigint>,
  # s_ map<text, text>);
  # ----
  # Any fields in the event that don't exist in the schema will be placed into 
  # the map of the appropriate type. The naming convention of the maps matches the way
  # http://docs.datastax.com/en/datastax_enterprise/4.7/datastax_enterprise/srch/srchDynFlds.html[Solr dynamic fields]
  # work in http://www.datastax.com/products/products-index[DataStax Enterprise], so
  # that with DSE, you can create a Solr schema on top of this table and then use
  # https://github.com/LucidWorks/banana/[Banana] to analyze logs stored in Cassandra.
  # Individual values stored in a map are limited to 64K. If you need to store larger
  # values, add a custom field (see below).
  #
  # It is also possible to customize the table by adding additional fields. Any
  # additional fields that match the name of those in your event will automatically
  # be used. When adding fields, make sure that their type matches the type used in
  # your event object, or the insert will fail.
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
