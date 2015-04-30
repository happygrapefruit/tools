# This utility is meant for use with Sift Science Score API.
# This code reads an input CSV file of user IDs (UIDs) and writes an output CSV file of UID & Sift Science Score.
# It returns a value of -1 if Sift does not have a score for a given UID.
# It includes a method that checks the UIDs already written when starting, so subsequent runs will only include new UIDs.

# Usage: API_KEY={your_API_key} ruby lookup.rb

require "csv"
require "sift"
require "set"

# input.csv **should not have any headers**
# Ensure input.csv is in same directory as this file.

# Sift's published API rate limit is 250 requests per second, which this process will typically not hit,
# however you can add sleep time in the format 0.05 = 5ms and use "time" in the command line to initially test.
# It's recommended that you begin with a smaller max_batch to ensure the process works.

INPUT_CSV          = "input.csv"
OUTPUT_CSV         = "output.csv"
REQUEST_SLEEP_TIME = 0
MAX_BATCH          = 10000

def lookup_uid(uid, client)
  response = client.score(uid)

# If Sift does not have a record of the UID (response status 54), return a value of -1.
# If any other response status besides expected 0 (okay) or 54 (not found), an error will be raised.

  if response.api_status == 0
    response.body["score"]
  elsif response.api_status == 54
    -1
  else
    raise "unexpected API status #{response.api_status}"
  end
end

def input_uids
  uids = Set.new

  CSV.foreach(INPUT_CSV) do |row|
    uids.add row[0]
  end

  uids
end

# If the process is interrupted, the next run picks up where you left off.

def known_uids
  uids = Set.new

  if File.exists?(OUTPUT_CSV)
    CSV.foreach(OUTPUT_CSV) do |row|
      uids.add(row[0])
    end
  end

  uids
end

client = Sift::Client.new(ENV.fetch("API_KEY"))

uids_for_lookup = (input_uids - known_uids).take(MAX_BATCH)

if uids_for_lookup.empty?
  puts "No more uids for lookup"
else
  CSV.open(OUTPUT_CSV, "ab") do |csv|
    uids_for_lookup.each do |uid|
      puts "Looking up uid #{uid}"
      score = lookup_uid(uid, client)
      csv << [uid, score]
      sleep REQUEST_SLEEP_TIME
    end
  end
end
