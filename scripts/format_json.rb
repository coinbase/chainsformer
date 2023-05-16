#!/usr/bin/env ruby

require "json"
require "neatjson"

def normalize(input)
  if input.is_a?(Array)
    input.each_with_object([]) do |v, a|
      a << normalize(v)
    end
  elsif input.is_a?(Hash)
    input.each_with_object({}) do |(k,v), h|
      v = normalize(v)
      h[k] = v unless v == {}
    end
  else
    input
  end
end

if ARGV.length < 1
  puts "Usage: format_json.rb FILE"
  exit 1
end

filename = ARGV[0]
input = JSON.parse(File.read(filename))
output = JSON.neat_generate(normalize(input), sort: true, wrap: 0, after_colon_1: 1, after_colon_n: 1)
File.open(filename, "w") do |file|
  file.write(output)
  file.write("\n")
end
