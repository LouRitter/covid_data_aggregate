require 'mongo'
require 'csv'
require 'open-uri'

include Mongo

DB_NAME = 'covid_db'
COLLECTION_NAME = 'covid_data'
CSV_URL = 'https://covid.ourworldindata.org/data/owid-covid-data.csv'

KEEP_FIELDS = %w[
  location continent date
  new_cases total_cases total_cases_per_million
  female_smokers male_smokers population
]

EXCLUDED_LOCATIONS = [
  "World", "Africa", "Asia", "Europe", "European Union", "High-income countries",
  "International", "Low-income countries", "Lower-middle-income countries", "North America",
  "Oceania", "South America", "Upper-middle-income countries"
]

def load_data
  puts "[*] Downloading CSV from: #{CSV_URL}"
  csv_data = URI.open(CSV_URL).read

  puts "[*] Parsing CSV..."
  parsed_data = CSV.parse(csv_data, headers: true)
  puts "[*] Total rows (excluding header): #{parsed_data.length}"

  puts "[*] Cleaning and filtering fields..."
  documents = parsed_data.map do |row|
    row.to_h.slice(*KEEP_FIELDS).transform_values { |v| v&.strip&.empty? ? nil : v }
  end

  puts "[*] Connecting to MongoDB..."
  client = Mongo::Client.new(['127.0.0.1:27017'], database: DB_NAME)
  collection = client[COLLECTION_NAME]
  collection.drop

  puts "[*] Inserting data in batches..."
  total = 0
  documents.each_slice(5000) do |batch|
    collection.insert_many(batch)
    total += batch.size
    print "\r[+] Inserted #{total} records..."
  end

  puts "\n[✓] All data inserted successfully. Total: #{total}"
end

def analyze
  File.open("output.txt", "w") do |file|
    client = Mongo::Client.new(['127.0.0.1:27017'], database: DB_NAME)
    collection = client[COLLECTION_NAME]

    file.puts "\n1. Total records in collection:"
    file.puts collection.count_documents({})

    puts "\n1. Total records in collection:"
    puts collection.count_documents({})

    latest_totals = collection.aggregate([
      { "$match" => { 
          "total_cases" => { "$ne" => nil, "$ne" => "" },
          "location" => { "$nin" => EXCLUDED_LOCATIONS }
        } 
      },
      { "$sort" => { "location" => 1, "date" => -1 } },
      { "$group" => {
          "_id" => "$location",
          "latest_total_cases" => { "$first" => { "$toDouble" => "$total_cases" } }
        }
      },
      { "$match" => { "latest_total_cases" => { "$ne" => nil } } },
      { "$group" => {
          "_id" => nil,
          "total" => { "$sum" => "$latest_total_cases" }
        }
      }
        ]).first

    file.puts "\n2. Total cumulative COVID-19 cases (latest available per country):"
    file.puts latest_totals ? latest_totals["total"].round : "N/A"
    puts "\n2. Total cumulative COVID-19 cases (latest available per country):"
    puts latest_totals ? latest_totals["total"].round : "N/A"

    file.puts "\n3. Countries and continents in dataset:"
    puts "\n3. Countries and continents in dataset:"

    countries = collection.distinct("location")
    continents = collection.distinct("continent").compact
    file.puts "Countries: #{countries.count}"
    file.puts "Continents: #{continents.count}"
    puts "Countries: #{countries.count}"
    puts "Continents: #{continents.count}"

    file.puts "\n4. Country with highest and lowest total COVID-19 cases per million (latest data only):"
    puts "\n4. Country with highest and lowest total COVID-19 cases per million (latest data only):"

    latest_per_million = collection.aggregate([
      { "$match" => {
          "total_cases_per_million" => { "$ne" => nil, "$ne" => "" },
          "location" => { "$nin" => EXCLUDED_LOCATIONS }
        }
      },
      { "$sort" => { "location" => 1, "date" => -1 } },
      { "$group" => {
          "_id" => "$location",
          "cases_per_million" => { "$first" => { "$toDouble" => "$total_cases_per_million" } }
        }
      },
      { "$sort" => { "cases_per_million" => -1 } },
    ]).to_a

    highest = latest_per_million.first
    lowest_non_zero = latest_per_million.reverse.find { |r| r["cases_per_million"] && r["cases_per_million"] > 0 }
    lowest_including_zero = latest_per_million.reverse.find { |r| r["cases_per_million"] }

    file.puts "Highest: #{highest['_id']} (#{highest['cases_per_million'].round})"
    puts "Highest: #{highest['_id']} (#{highest['cases_per_million'].round})"

    if lowest_non_zero && lowest_non_zero["cases_per_million"]
      file.puts "Lowest (non-zero): #{lowest_non_zero['_id']} (#{lowest_non_zero['cases_per_million'].round})"
      puts "Lowest (non-zero): #{lowest_non_zero['_id']} (#{lowest_non_zero['cases_per_million'].round})"
    else
      file.puts "Lowest (non-zero): N/A"
      puts "Lowest (non-zero): N/A"
    end
    if lowest_including_zero && lowest_including_zero["cases_per_million"]
      file.puts "Lowest (including zero): #{lowest_including_zero['_id']} (#{lowest_including_zero['cases_per_million'].round})"
      puts "Lowest (including zero): #{lowest_including_zero['_id']} (#{lowest_including_zero['cases_per_million'].round})"
    else
      file.puts "Lowest (including zero): N/A"
      puts "Lowest (including zero): N/A"
    end

    zero_cases_countries = latest_per_million.select { |r| r["cases_per_million"] == 0 }.map { |r| r["_id"] }

    file.puts "\nCountries with total_cases_per_million = 0:"
    puts "\nCountries with total_cases_per_million = 0:"

    zero_cases_countries.each { |c| file.puts "- #{c}" }
    
    file.puts "\n5. Day with highest number of new cases:"
    puts "\n5. Day with highest number of new cases:"

    peak = collection.aggregate([
      { "$match" => {
          "new_cases" => { "$ne" => nil, "$ne" => "" },
          "location" => { "$nin" => EXCLUDED_LOCATIONS }
      }},
      { "$project" => { "location" => 1, "date" => 1, "new_cases" => { "$toDouble" => "$new_cases" } } },
      { "$sort" => { "new_cases" => -1 } },
      { "$limit" => 1 }
    ]).first
    file.puts "#{peak['location']} on #{peak['date']} with #{peak['new_cases'].round} cases"
    puts "#{peak['location']} on #{peak['date']} with #{peak['new_cases'].round} cases"

    file.puts "\n6. Smoking stats (latest available):"
    puts "\n6. Smoking stats (latest available):"

    highest_male = collection.aggregate([
      { "$match" => {
          "male_smokers" => { "$ne" => nil, "$ne" => "" },
          "location" => { "$nin" => EXCLUDED_LOCATIONS }
      }},
      { "$sort" => { "location" => 1, "date" => -1 } },
      { "$group" => {
          "_id" => "$location",
          "male_smokers" => { "$first" => { "$toDouble" => "$male_smokers" } }
      }},
      { "$sort" => { "male_smokers" => -1 } },
      { "$limit" => 1 }
    ]).first

    highest_female = collection.aggregate([
      { "$match" => {
          "female_smokers" => { "$ne" => nil, "$ne" => "" },
          "location" => { "$nin" => EXCLUDED_LOCATIONS }
      }},
      { "$sort" => { "location" => 1, "date" => -1 } },
      { "$group" => {
          "_id" => "$location",
          "female_smokers" => { "$first" => { "$toDouble" => "$female_smokers" } }
      }},
      { "$sort" => { "female_smokers" => -1 } },
      { "$limit" => 1 }
    ]).first

    estimated_smokers = collection.aggregate([
      { "$match" => {
          "population" => { "$ne" => nil, "$ne" => "" },
          "male_smokers" => { "$ne" => nil, "$ne" => "" },
          "female_smokers" => { "$ne" => nil, "$ne" => "" },
          "location" => { "$nin" => EXCLUDED_LOCATIONS }
      }},
      { "$sort" => { "location" => 1, "date" => -1 } },
      { "$group" => {
          "_id" => "$location",
          "population" => { "$first" => { "$toDouble" => "$population" } },
          "male_smokers" => { "$first" => { "$toDouble" => "$male_smokers" } },
          "female_smokers" => { "$first" => { "$toDouble" => "$female_smokers" } }
      }},
      { "$project" => {
          "estimated_total_smokers" => {
            "$add" => [
              { "$multiply" => [ { "$divide" => ["$population", 2] }, { "$divide" => ["$male_smokers", 100] } ] },
              { "$multiply" => [ { "$divide" => ["$population", 2] }, { "$divide" => ["$female_smokers", 100] } ] }
            ]
          }
      }},
      { "$sort" => { "estimated_total_smokers" => -1 } },
      { "$limit" => 1 }
    ]).first
    puts estimated_smokers.to_yaml
    file.puts "Highest % of male smokers: #{highest_male['_id']} – #{highest_male['male_smokers']}%"
    file.puts "Highest % of female smokers: #{highest_female['_id']} – #{highest_female['female_smokers']}%"
    file.puts "Estimated country with highest number of smokers (assuming 50/50 gender split): #{estimated_smokers['_id']} – #{estimated_smokers['estimated_total_smokers'].round} smokers"
    puts "Highest % of male smokers: #{highest_male['_id']} – #{highest_male['male_smokers']}%"
    puts "Highest % of female smokers: #{highest_female['_id']} – #{highest_female['female_smokers']}%"
    puts "Estimated country with highest number of smokers (assuming 50/50 gender split): #{estimated_smokers['_id']} – #{estimated_smokers['estimated_total_smokers'].round} smokers"
    
    puts "\n[✓] Analysis complete. Results saved to output.txt"

  end
end

if ARGV.empty?
  puts "Usage: ruby code.rb [load_data|analyze]"
elsif ARGV[0] == 'load_data'
  load_data
elsif ARGV[0] == 'analyze'
  analyze
else
  puts "Invalid command. Use 'load_data' or 'analyze'."
end
