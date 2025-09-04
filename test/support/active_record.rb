require "active_record"

logger = ActiveSupport::Logger.new(ENV["VERBOSE"] ? STDOUT : nil)

ActiveRecord::Base.logger = logger
ActiveRecord::Migration.verbose = ENV["VERBOSE"]

case ENV["ADAPTER"]
when "postgresql"
  ActiveRecord::Base.establish_connection adapter: "postgresql", database: "polars_ruby_test"
when "mysql"
  ActiveRecord::Base.establish_connection adapter: "mysql2", database: "polars_ruby_test"
when "trilogy"
  ActiveRecord::Base.establish_connection adapter: "trilogy", database: "polars_ruby_test", host: "127.0.0.1"
else
  ActiveRecord::Base.establish_connection adapter: "sqlite3", database: ":memory:"
end

if ActiveSupport::VERSION::STRING.to_f == 8.0
  ActiveSupport.to_time_preserves_timezone = :zone
elsif ActiveSupport::VERSION::STRING.to_f == 7.2
  ActiveSupport.to_time_preserves_timezone = true
end

ActiveRecord::Schema.define do
  create_table :users, force: true do |t|
    t.string :name
    t.integer :number
    t.float :inexact
    t.boolean :active
    t.datetime :joined_at
    t.date :joined_on
    t.binary :bin
    t.decimal :dec, precision: 10, scale: 3
    t.text :txt
    t.time :joined_time
    if ENV["ADAPTER"] == "postgresql"
      t.column :settings, :jsonb
    else
      t.column :settings, :json
    end
  end
end

class User < ActiveRecord::Base
end
