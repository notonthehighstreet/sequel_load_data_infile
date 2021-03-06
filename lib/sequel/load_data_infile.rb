require 'sequel'

module Sequel
  # @api private
  class LoadDataInfileExpression
    attr_reader :path, :table, :columns, :ignore, :character_set, :local
    
    def initialize(path, table, columns, opts={})
      @path    = path
      @table   = table
      @columns = columns
      @ignore  = opts[:ignore]
      @update  = opts[:update]
      @local   = opts[:local]
      @set     = opts[:set] || {}
      @character_set = opts[:character_set] || "utf8"
      if opts[:format] == :csv
        @field_terminator = ","
        @enclosed_by = '"'
        @escaped_by = '"'
      end
    end
    
    def replace?
      @update == :replace
    end

    def ignore?
      @update == :ignore
    end

    def to_sql(db)
      @db = db
      
      [load_fragment,
       replace_fragment,
       table_fragment,
       character_set_fragment,
       field_terminator_fragment,
       field_enclosure_fragment,
       escape_fragment,
       ignore_fragment, 
       column_fragment,
       set_fragment].compact.join(" ")
    end

    private

    def load_fragment
      "LOAD DATA #{local_fragment}INFILE '#{path}'"
    end

    def replace_fragment
      @update.to_s.upcase if replace? || ignore?
    end

    def table_fragment
      "INTO TABLE `#{table}`"
    end

    def character_set_fragment
      "CHARACTER SET '#{character_set}'"
    end

    def field_terminator_fragment
      "FIELDS TERMINATED BY '#{@field_terminator}'" if @field_terminator
    end

    def field_enclosure_fragment
      "OPTIONALLY ENCLOSED BY '#{@enclosed_by}'" if @enclosed_by
    end

    def escape_fragment
      "ESCAPED BY '#{@escaped_by}'" if @escaped_by
    end

    def ignore_fragment
      "IGNORE #{ignore} LINES" if ignore
    end

    def local_fragment
      "LOCAL " if local
    end

    def column_fragment
      "(" + columns.map {|c| format_column(c) }.join(",") + ")"
    end

    def set_fragment
      unless set_columns.empty?
        "SET " + set_columns.map do |k, v|
          "#{@db.literal(k)} = #{@db.literal(v)}"
        end.join(", ")
      end
    end

    def format_column(column)
      if binary_columns.include?(column.to_s)
        "@#{column}"
      elsif column.to_s[0..0] == "@"
        column
      else
        "`#{column}`"
      end
    end

    def binary_columns
      @binary_columns ||= @db.schema(@table).
        select {|a| a[1][:db_type] =~ /^binary/ }.map {|a| a.first.to_s }
    end
    
    def set_columns
      binary_columns.inject({}) do |hash, column|
        hash[column.to_sym] = Sequel.function(:unhex, 
                                              Sequel.lit("@#{column}"))
        hash
      end.merge(@set)
    end
  end

  module LoadDataInfile
    # Load data in file specified at path.
    #
    # Columns is a list of columns to load - column names starting
    # with an @ symbol will be treated as variables.
    #
    # By default, this will generate a REPLACE INTO TABLE
    # statement.
    #
    # Options:
    # :ignore - the number of lines to ignore in the source file
    # :update - nil, :ignore or :replace
    # :set - a hash specifying autopopulation of columns
    # :character_set - the character set of the file, UTF8 default
    # :format - either nil or :csv
    def load_infile(path, columns, options={})
      execute_dui(load_infile_sql(path, columns, options))
    end

    # Returns the SQL for a LOAD DATA INFILE statement.
    def load_infile_sql(path, columns, options={})
      replacement = opts[:insert_ignore] ? :ignore : :replace
      options = {:update => replacement}.merge(options)
      LoadDataInfileExpression.new(path, 
                                   opts[:from].first, 
                                   columns, 
                                   options).
        to_sql(db)
    end

    # Loads the CSV data columns in path into this dataset's
    # table.
    #
    # See load_infile for more options.
    def load_csv_infile(path, columns, options={})
      execute_dui(load_csv_infile_sql(path, columns, options))
    end
    
    def load_csv_infile_sql(path, columns, options={})
      load_infile_sql(path, columns, options.merge(:format => :csv))
    end
  end
end

Sequel::Dataset.send :include, Sequel::LoadDataInfile
