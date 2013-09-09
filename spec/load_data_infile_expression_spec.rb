require File.expand_path(File.dirname(__FILE__) + '/spec_helper')

describe Sequel::LoadDataInfileExpression do
  before :each do
    TEST_DB.stub(:schema).and_return([])
  end

  it "loads the data in the file into the table" do
    described_class.new("bar.csv", :foo, ['bar', 'quux']).
      to_sql(TEST_DB).should include("LOAD DATA INFILE 'bar.csv' INTO TABLE `foo`")
  end

  it "loads the data with replacment" do
    described_class.new("bar.csv", :foo, ['bar', 'quux'],
                        :update => :replace).
      to_sql(TEST_DB).should include("REPLACE INTO TABLE")
  end

  it "loads the data ignoring rows" do
    described_class.new("bar.csv", :foo, ['bar', 'quux'], :update => :ignore).
      to_sql(TEST_DB).should include("IGNORE INTO TABLE")
  end

  it "should be in UTF-8 character set by default" do
    described_class.new("bar.csv", :foo, ['bar', 'quux']).
      to_sql(TEST_DB).should include("CHARACTER SET 'utf8'")
  end

  it "may be in other character sets" do
    described_class.new("bar.csv", :foo, ['bar', 'quux'], :character_set => "ascii").
      to_sql(TEST_DB).should include("CHARACTER SET 'ascii'")
  end

  it "should load columns" do
    described_class.new("bar.csv", :foo, ['bar', 'quux']).
      to_sql(TEST_DB).should include("(`bar`,`quux`)")
  end

  it "should load into variables if column begins with @" do
    described_class.new("bar.csv", :foo, ['@bar', 'quux']).
      to_sql(TEST_DB).should include("(@bar,`quux`)")
  end

  it "can ignore lines" do
    described_class.new("bar.csv", :foo, ['bar', 'quux'], :ignore => 2).
      to_sql(TEST_DB).should include("IGNORE 2 LINES")
  end

  it "can be in csv format" do
    described_class.new("bar.csv", :foo, ['bar', 'quux'], :format => :csv).
      to_sql(TEST_DB).should include("FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' ESCAPED BY '\"'")
  end

  it "can set column values" do
    sql = described_class.new("bar.csv", :foo, ['@bar', 'quux'], 
                        :set => {:bar => Sequel.function(:unhex, Sequel.lit("@bar")),
                        :etl_batch_id => 3}).
      to_sql(TEST_DB)

    
    sql.should include("`etl_batch_id` = 3")
    sql.should include("`bar` = unhex(@bar)")
  end

  it "unhexes binary columns automatically via set" do
    TEST_DB.stub(:schema).and_return([[:bar, {:type => :blob, :db_type => "binary(16)"}]])
    sql = described_class.new("bar.csv", :foo, [:bar, :quux]).to_sql(TEST_DB)
    sql.should include("(@bar,`quux`)")
    sql.should include("SET `bar` = unhex(@bar)")
  end

  it "doesn't trust Sequel's type conversion" do
    TEST_DB.stub(:schema).and_return([[:bar, {:type => :blob, :db_type => "enum('foo')"}]])
    sql = described_class.new("bar.csv", :foo, [:bar, :quux]).to_sql(TEST_DB)
    sql.should_not include("(@bar,`quux`)")
    sql.should_not include("SET `bar` = unhex(@bar)")
  end
end
