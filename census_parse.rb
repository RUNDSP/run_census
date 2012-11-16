# ---------------------------------------------------------------------------------------------------------
#
# Three major portions:
#   1) Retrieve files and uncompress
#   2A) Create a database with its tables 
#   2B) Preprocess SF1 files to create CS1 (semi-filtered CSV files); 
#             geoheader file gets exact match on SUMLEV = 880 (characters 9-11) 
#   2B) Load tables
#   3) Query the database to get all fields 
#
# ---------------------------------------------------------------------------------------------------------

require 'JSON'
require 'mysql2'
require 'open-uri'
require 'rubygems'
require 'zip/zip'
require 'zip/zipfilesystem'

# ---------------------------------------------------------------------------------------------------------
#
# Hardcoded variables 
# 
# Data Sets:
#  SF1 (2010 US Census Summary File 1) 
#  ??? (2010 US Census Rural and Urban Updates)  - same data structures as SF1
#  ACS (2010 American Community Survey)  - unknown data types
#
# Tables: 
#  177 population tables (identified with a " P" ) shown down to the block level
#  58 housing tables (identified with an " H" ) shown down to the block level
#  82 population tables (identified with a " PCT" ) shown down to census tract level; 
#  4 housing tables (identified with an HCT) shown down to census tract level; 
# shown down to the census tract level; and 10 population tables (identified with a PCO) shown down to the county level, 
# for a total of 331 tables. 
#
# There are 14 population tables and 4 housing tables shown down to the block level and 5 population tables shown 
# down to the census tract level that are repeated by the major race and Hispanic or Latino groups.
#
# Some tables only go to county level 
#
# ---------------------------------------------------------------------------------------------------------

ENCODING    = 'latin-1'                          # Government source data is latin-1. 
TOTSEGS     = 48                                 # 
TOTTABLES   = 331                                # 
TOTVARS     = 9147                               # Raw number of variables, not all used in all datasets
NUMVARS     = 8912                               # Net = NUMVARS - 47*5 duped fields FILEID,STUSAB,CHARITER,CIFSN,LOGRECNO)
SUMLEV      = '880'                              # Make sure it's a string not a number 

SQLSCRIPT_D   = '/Users/alexfarrill/Downloads/RunDSP'
working_dir   = '/Users/alexfarrill/Downloads/RunDSP/out'
col_desc_file   = '/Users/alexfarrill/Downloads/RunDSP/sf1_labels.json'

MYSQLBIN_D    = '/usr/local/bin'
SQLHOST      = 'localhost'
SQLUSER     = 'root'
SQLPASSWORD = ''
SQLDATABASE = 'rcensus'
SQLCHUNK    = 5000

#uri1     = 'ftp://ftp.census.gov/census_2010/04-Summary_File_1/Montana/mt2010.sf1.zip'
uri1    = 'ftp://ftp.census.gov/census_2010/04-Summary_File_1/National/us2010.sf1.zip'
uri2    = 'ftp://ftp.census.gov/census_2010/04-Summary_File_1/Urban_Rural_Update/National/us2010.ur1.zip'

# ---------------------------------------------------------------------------------------------------------
#
# Download ZIP file from Census website and uncompress in current directory
#
# ---------------------------------------------------------------------------------------------------------
def download_zip(uri,working_dir)
    begin
    source = open(uri)       
      begin
        cf = File.open(source)
        file_cnt = 0
        Zip::ZipFile.open(cf) do |zipfile|
          zipfile.each do |file|
            puts "uncompressing #{file}"  # this works
            zipfile.extract(file, "#{working_dir}/#{file.name}") unless File.exist?("#{working_dir}/#{file.name}")
            file_cnt += 1
          end
           
          end
          if file_cnt != (TOTSEGS + 1)
            puts "Error - expecting #{TOTSEGS} files - received #{file_cnt}"
            puts "Rerun script"
            exit
          end

        rescue StandardError=>e
          puts "Error: status code #{e}"  
        end
    rescue StandardError=>e
      puts "Error: status code #{e}"  
    end
end

# ---------------------------------------------------------------------------------------------------------
# Create tables as needed
#   1) ruby mysql2 should work but means rewriting 9000 SQL lines
#   2) system call to mysql
#
# ---------------------------------------------------------------------------------------------------------
def create_census_tables(host,user,password,database,sqlscripts)
  sql_str = "#{MYSQLBIN_D}/mysql -h #{host} -u #{user} --password=#{password} -D #{database} < #{sqlscripts}/create_all_sf1.sql"
  puts sql_str
  begin
    system(sql_str)
  rescue
    puts "SQL statement failed: #{sql_str}"
  end
end

#
# Preprocess tables to only include the range of records of interest 
#  1) Exact match for geographic header file (geo2010)
#  2) Range match (between min and max LOGRECNO)
# 
# Assumes that LOGRECNO are consecutive in all source files
# 
def preprocess_tables(work_dir, dataset)
  suffix = dataset                       # Either SF1 or Rural/Urban Update (RU) 
                                         # Hoping that the ACS has same data dictionary
  zcta_logrecno = []

  # Filter the usgeo2010 file to make life easier and should result in 44,410 records
  go_file = File.join work_dir, "usgeo2010.csv"
  FileUtils.rm(go_file)
  go = File.open go_file, "w"
  
  gi_file = File.join work_dir, "usgeo2010.#{dataset}"
  gi = File.open(gi_file,"r") 
  raise "#{gi_file} not found or empty" unless File.size(gi)

  while line = gi.gets do 
    next if line[8,3] != SUMLEV          # SF1 data dictionary - SUMLEV = 880 for zcta data
    go.puts line
    thisRecno = line[18,7]
    zcta_logrecno << thisRecno           # record all the matched records
  end
  
  puts "#{zcta_logrecno.length} ZCTA records found"
  rec_min = zcta_logrecno.min
  rec_max = zcta_logrecno.max
  puts "records between #{rec_min} and #{rec_max}"

  (1..47).each do |i|  
    fo = work_dir + "/us000%02d" %  i + "2010.cs1"
    fcsv = File.open(work_dir + "/us000%02d" %  i + "2010.csv","w")
    fi = work_dir + "/us000%02d" %  i + "2010.#{dataset}"    

    next if File.size?(fi)
    puts "Filtering file #{fi}"

    `/usr/bin/awk \'/#{rec_min}/, /#{rec_max}/\' #{fi} > #{fo}`
    #
    # Filter for exact LOGRECNO match only
    #

    #csv_array = CSV.read(fo)             # read whole file into memory
    #csv_array.each do |inrow|
    #  thisRecno = inrow[4]               # Make sure this is a string

    #  if zcta_logrecno.include?(thisRecno)  # Really slow, faster to import and join in SQL
    #    fcsv.puts inrow.join(',')
    #  end
    end                                       # filter_tables definitions 
end

# ---------------------------------------------------------------------------------------------------------
# Load the database with one fixed-format file and 47 comma-delimited files
# mysql2 gem should work but doesn't
#
# ---------------------------------------------------------------------------------------------------------
def load_tables(client, working_dir, suffix, dataset)
  # Either SF1 or Rural/Urban Update (RU) 
  # Hoping that the ACS has same data dictionary

  # use mysql to load geo table
  #
  f = File.join working_dir, "usgeo2010.csv"
  t = "geo2010"

  qstr = "\"LOAD DATA INFILE \'#{f}\' INTO TABLE #{t} FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n' "
  qstr << "
(@var)
SET FILEID = SUBSTR(@var,1 , 6 ),
STUSAB = SUBSTR(@var,7 , 2 ),
SUMLEV = SUBSTR(@var,9 , 3 ),
GEOCOMP = SUBSTR(@var,12 , 2 ),
CHARITER = SUBSTR(@var,14 , 3 ),
CIFSN = SUBSTR(@var,17 , 2 ),
LOGRECNO = SUBSTR(@var,19 , 7 ),
REGION = SUBSTR(@var,26 , 1 ),
DIVISION = SUBSTR(@var,27 , 1 ),
STATE = SUBSTR(@var,28 , 2 ),
COUNTY = SUBSTR(@var,30 , 3 ),
COUNTYCC = SUBSTR(@var,33 , 2 ),
COUNTYSC = SUBSTR(@var,35 , 2 ),
COUSUB = SUBSTR(@var,37 , 5 ),
COUSUBCC = SUBSTR(@var,42 , 2 ),
COUSUBSC = SUBSTR(@var,44 , 2 ),
PLACE = SUBSTR(@var,46 , 5 ),
PLACECC = SUBSTR(@var,51 , 2 ),
PLACESC = SUBSTR(@var,53 , 2 ),
TRACT = SUBSTR(@var,55 , 6 ),
BLKGRP = SUBSTR(@var,61 , 1 ),
BLOCK = SUBSTR(@var,62 , 4 ),
IUC = SUBSTR(@var,66 , 2 ),
CONCIT = SUBSTR(@var,68 , 5 ),
CONCITCC = SUBSTR(@var,73 , 2 ),
CONCITSC = SUBSTR(@var,75 , 2 ),
AIANHH = SUBSTR(@var,77 , 4 ),
AIANHHFP = SUBSTR(@var,81 , 5 ),
AIANHHCC = SUBSTR(@var,86 , 2 ),
AIHHTLI = SUBSTR(@var,88 , 1 ),
AITSCE = SUBSTR(@var,89 , 3 ),
AITS = SUBSTR(@var,92 , 5 ),
AITSCC = SUBSTR(@var,97 , 2 ),
TTRACT = SUBSTR(@var,99 , 6 ),
TBLKGRP = SUBSTR(@var,105 , 1 ),
ANRC = SUBSTR(@var,106 , 5 ),
ANRCCC = SUBSTR(@var,111 , 2 ),
CBSA = SUBSTR(@var,113 , 5 ),
CBSASC = SUBSTR(@var,118 , 2 ),
METDIV = SUBSTR(@var,120 , 5 ),
CSA = SUBSTR(@var,125 , 3 ),
NECTA = SUBSTR(@var,128 , 5 ),
NECTASC = SUBSTR(@var,133 , 2 ),
NECTADIV = SUBSTR(@var,135 , 5 ),
CNECTA = SUBSTR(@var,140 , 3 ),
CBSAPCI = SUBSTR(@var,143 , 1 ),
NECTAPCI = SUBSTR(@var,144 , 1 ),
UA = SUBSTR(@var,145 , 5 ),
UASC = SUBSTR(@var,150 , 2 ),
UATYPE = SUBSTR(@var,152 , 1 ),
UR = SUBSTR(@var,153 , 1 ),
CD = SUBSTR(@var,154 , 2 ),
SLDU = SUBSTR(@var,156 , 3 ),
SLDL = SUBSTR(@var,159 , 3 ),
VTD = SUBSTR(@var,162 , 6 ),
VTDI = SUBSTR(@var,168 , 1 ),
RESERVE2 = SUBSTR(@var,169 , 3 ),
ZCTA5 = SUBSTR(@var,172 , 5 ),
SUBMCD = SUBSTR(@var,177 , 5 ),
SUBMCDCC = SUBSTR(@var,182 , 2 ),
SDELM = SUBSTR(@var,184 , 5 ),
SDSEC = SUBSTR(@var,189 , 5 ),
SDUNI = SUBSTR(@var,194 , 5 ),
AREALAND = SUBSTR(@var,199 , 14 ),
AREAWATR = SUBSTR(@var,213 , 14 ),
NAME = SUBSTR(@var,227 , 90 ),
FUNCSTAT = SUBSTR(@var,318 , 1 ),
GCUNI = SUBSTR(@var,319 , 1 ),
POP100 = SUBSTR(@var,319 , 9 ),
HU100 = SUBSTR(@var,328, 9 ),
INTPTLAT = SUBSTR(@var,337 , 11 ),
INTPTLON = SUBSTR(@var,348 , 12 ),
LSADC = SUBSTR(@var,360 , 2 ),
PARTFLAG = SUBSTR(@var,362 , 1 ),
RESERVE3 = SUBSTR(@var,363 , 6 ),
UGA = SUBSTR(@var,369 , 5 ),
STATENS = SUBSTR(@var,374 , 8 ),
COUNTYNS = SUBSTR(@var,381 , 8 ),
COUSUBNS = SUBSTR(@var,390 , 8 ),
PLACENS = SUBSTR(@var,398 , 8 ),
CONCITNS = SUBSTR(@var,406 , 8 ),
AIANHHNS = SUBSTR(@var,414 , 8 ),
AITSNS = SUBSTR(@var,421 , 8 ),
ANRCNS = SUBSTR(@var,430 , 8 ),
SUBMCDNS = SUBSTR(@var,438 , 8 ),
CD113 = SUBSTR(@var,446 , 2 ),
CD114 = SUBSTR(@var,448 , 2 ),
CD115 = SUBSTR(@var,450 , 2 ),
SLDU2 = SUBSTR(@var,452 , 3 ),
SLDU3 = SUBSTR(@var,455 , 3 ),
SLDU4 = SUBSTR(@var,458 , 3 ),
SLDL2 = SUBSTR(@var,461 , 3 ),
SLDL3 = SUBSTR(@var,464 , 3 ),
SLDL4 = SUBSTR(@var,467 , 3 ),
AIANHHSC = SUBSTR(@var,470 , 2 ),
CSASC = SUBSTR(@var,472 , 2 ),
CNECTASC = SUBSTR(@var,474 , 2 ),
MEMI = SUBSTR(@var,476 , 1 ),
NMEMI = SUBSTR(@var,477 , 1 ),
PUMA = SUBSTR(@var,478 , 5 ),
RESERVED = SUBSTR(@var,483 , 18 ); 
\""

  sql_str = "#{MYSQLBIN_D}/mysql -h #{SQLHOST} -u #{SQLUSER} --password=#{SQLPASSWORD} -D #{SQLDATABASE} --execute=#{qstr}"
  
  system(sql_str)

  (1..47).each do |i|
    t = "#{dataset}_%02d" % i
    cnt = client.query("select count(*) cnt from #{t}").first["cnt"]
    
    next if cnt > 0
    f = working_dir + "/us000%02d" %  i + "2010.#{suffix}"

    unless File.size?(f)
      puts "File #{f} doesn't exist!" 
      next
    end
    
    qstr = "LOAD DATA INFILE '#{f}' INTO TABLE #{t} FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n';"
    puts qstr
    
    client.query(qstr)
  end
end


# ---------------------------------------------------------------------------------------------------------
# Lookup column name in the JSON data structure, traverse up the tree to get the tablename => table descriptor
#   
#{
# "table": "PCT12G", 
# "name": "SEX BY AGE (TWO OR MORE RACES)",
# "universe": "People who are Two or More Races",
# "text": "12 years"
#}
#
# ---------------------------------------------------------------------------------------------------------
def json_headers(col_desc_h, key)
  hz = {}
  
  table = key[0...-3].gsub(/([A-Za-z]+)0+([1-9]+)/, '\1\2')

  # puts "table: #{table}"
  # puts "col_desc_h[table]: #{col_desc_h[table]}"
  # puts "key: #{key}"
  
  if col_desc_h[table]
    # hz[:table] = table
    hz[:name] = col_desc_h[table]["name"]
    hz[:universe] = col_desc_h[table]["universe"]
    hz[:text] = col_desc_h[table]["labels"][key]["text"]
    hz
  else
    puts "table not found: #{table}"
    nil
  end
end

# ---------------------------------------------------------------------------------------------------------
# Query the database for all variables
#    Divide the query results into manageable chunks using chunks and record offset
#
# ---------------------------------------------------------------------------------------------------------

def query_census(client, chunk, rec_offset, file, col_desc_h)

  qstr1 = "SELECT  g.state, g.county, g.zcta5, "
  qstr2 = "FROM geo2010 g "
     
  1.upto(46) do |x|
    #  next if x == 40 or x == 41                           # Empty segment at ZCTA level
    qstr1 += "s%02d.*, " % (x)
    qstr2 += "LEFT JOIN sf1_%02d " % (x) + "s%02d " % (x) + "on g.LOGRECNO = s%02d" % (x) + ".LOGRECNO  "
  end
  
  qstr1 += " s47.* "
  qstr2 +=   "LEFT JOIN sf1_47 s47 on g.LOGRECNO = s47.LOGRECNO where g.SUMLEV = '880' ORDER BY g.LOGRECNO LIMIT #{chunk} OFFSET #{rec_offset} ;"

  qstring = qstr1 + qstr2
  puts qstring.inspect

  results = client.query(qstring)

  #
  # Each row is a hash with the keys as the fields 
  # Parse the array to skip the extra header fields present in all data files (FILEID, STUSAB, CHARITER, CIFSN)
  #                    keep the initial geographic header fields,     
  #
  # For first four fields (state,county, zcta,LOGRECNO) just print values
  # All other fields print two descriptor and value
  #  "P008030": {
  #     "descriptor": {...}  
  #     "value": VALUE 
  #  }
  #
  # 

  puts "Writing JSON with records"
  results.each(:as => hash, :cache_rows => false) do |row|
    h = {}
    
    for (key, value) in row do
      next if key =~ /FILEID|STUSAB|CHARITER|CIFSN/      # Repeated for every table from wildcard select
      
      if key =~ /state|county|zcta|LOGRECNO/
        h[key] = value if key == "zcta5"
      elsif d = json_headers(col_desc_h, key)
        h.merge! d
        h[:key] = key
        h[:value] = value
      end
    end
    
    file.print h.to_json + "\n"
  end
end




# ---------------------------------------------------------------------------------------------------------
#  MAIN
#
# ---------------------------------------------------------------------------------------------------------

puts "downloading from #{uri1}"
#download_zip(uri1, working_dir)                     # ftp and uncompress SF1 data

begin
  client = Mysql2::Client.new( :host => SQLHOST, :username => SQLUSER,  :password => SQLPASSWORD, :database => SQLDATABASE)
rescue
  raise "Could not connect to database #{SQLDATABASE}"
end

puts "Creating tables"
create_census_tables(SQLHOST,SQLUSER,SQLPASSWORD,SQLDATABASE,SQLSCRIPT_D)

puts "Preprocessing files"
preprocess_tables(working_dir, "sf1")

puts "Loading tables"
load_tables(client, working_dir, "cs1", "sf1")

begin
  col_desc_f = File.open(col_desc_file, 'r')
rescue
  raise "Could not open #{col_desc_file}"
end

raise "Could not read file #{col_desc_file}" unless File.size?(col_desc_f)
json = File.read(col_desc_f)

col_desc_hash = JSON.parse(json)
  
puts "Querying database"

offset = 0
max_offset = 44100 - SQLCHUNK

json_output_file =  File.open(File.join(working_dir, "census.json"), "w")

while offset < max_offset do
  puts "Running query with LIMIT #{SQLCHUNK} offset #{offset} "
  query_census(client, SQLCHUNK, offset, json_output_file, col_desc_hash)
  offset += SQLCHUNK
end