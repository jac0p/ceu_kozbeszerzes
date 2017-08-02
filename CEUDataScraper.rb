#!/usr/bin/ruby

require 'rubygems'
require 'logger'
require 'nokogiri'
require 'open-uri'
require 'net/http'

module CEUDataScraper
  
  class CSVFiller
    attr_reader :requested_files, :bid_files, :won_files

    def initialize(requested_files, bid_files, won_files)
      @logger           = Logger.new(STDOUT)
      @requested_files  = requested_files
      @bid_files        = bid_files
      @won_files        = won_files
      @csv_req_master   = "/home/buub/Scriptland/Scrapes/CEU_Kozbeszerzes/csv_files/req_master.csv"
      @csv_bid_master   = "/home/buub/Scriptland/Scrapes/CEU_Kozbeszerzes/csv_files/bid_master.csv"
      @csv_won_master   = "/home/buub/Scriptland/Scrapes/CEU_Kozbeszerzes/csv_files/won_master.csv"
    end

    def collect_requested
      @requested_files.each do |file|
        @logger.info "checking file: #{file}"
        if File.size(file) > 73
          @logger.info "File contains serviceable records"
          File.open(file, "r").each_line do |line| 
            File.open(@csv_req_master, "a") { |master| master.write(line) }
          end
        elsif File.size(file) <= 73
          @logger.warn "File size indicates lack of serviceable records"
        else
          @logger.error "Unexpected error reading file"
        end
      end
      collect_bid
    end

    def collect_bid
      @bid_files.each do |file|
        @logger.info "checking file: #{file}"
        if File.size(file) > 73
          @logger.info "File contains serviceable records"
          File.open(file, "r").each_line do |line| 
            File.open(@csv_bid_master, "a") { |master| master.write(line) }
          end      
        elsif File.size(file) <= 73
          @logger.warn "File size indicates lack of serviceable records"
        else
          @logger.error "Unexpected error reading file"
        end
      end
      collect_won
    end

    def collect_won
      @won_files.each do |file|
        @logger.info "checking file: #{file}"
        if File.size(file) > 73
          @logger.info "File contains serviceable records"
          File.open(file, "r").each_line do |line| 
            File.open(@csv_won_master, "a") { |master| master.write(line) }
          end      
        elsif File.size(file) <= 73
          @logger.warn "File size indicates lack of serviceable records"
        else
          @logger.error "Unexpected error reading file"
        end
      end
    end

  end

  class FolderReader
    attr_reader :req_destination, :bid_destination, :won_destination

    def initialize(req_destination, bid_destination, won_destination)
      @logger           = Logger.new(STDOUT)
      @req_destination  = req_destination
      @bid_destination  = bid_destination
      @won_destination  = won_destination
    end

    def list_requested
      @requested_files = []
      list_requested = `find #{@req_destination} -type f`
      list_requested.each_line do |csv|
        @requested_files << csv.chomp
      end
      list_bid
    end

    def list_bid
      @bid_files = []
      list_bid = `find #{@bid_destination} -type f`
      list_bid.each_line do |csv|
        @bid_files << csv.chomp
      end
      list_won
    end

    def list_won
      @won_files = []
      list_won = `find #{@won_destination} -type f`
      list_won.each_line do |csv|
        @won_files << csv.chomp
      end

      Wrapper.fill_wrapper(
        :requested_files  => @requested_files,
        :bid_files        => @bid_files,
        :won_files        => @won_files
      )

    end
  end

  class Scraper
    attr_reader :base_url, :req_destination, :bid_destination, :won_destination, :company_matrix

    def initialize(base_url, req_destination, bid_destination, won_destination, company_matrix)
      @logger           = Logger.new(STDOUT)
      @base_url         = base_url
      @req_destination  = req_destination
      @bid_destination  = bid_destination
      @won_destination  = won_destination
      @company_matrix   = company_matrix
    end

    def detect_redirections
      (10000000..10000001).each do |entity_id|
        @url              = "#{@base_url}/entity/t/#{entity_id}.xml"
        @url_requested    = "#{@base_url}/data/csv/entity/all_tenders_requested/t_#{entity_id}.windows-1250.csv"
        @url_won          = "#{@base_url}/data/csv/entity/all_tenders_won/t_#{entity_id}.windows-1250.csv"
        @url_bid          = "#{@base_url}/data/csv/entity/all_tenders_bid/t_#{entity_id}.windows-1250.csv"
        
        @logger.info "Looking for entity_id: #{entity_id}"
        resp = Net::HTTP.get_response(URI.parse(@url))
        if resp.code.match(/20\d/)
          @logger.info "entity_id found"
          get_company_name(entity_id)
        else
          @logger.warn "entity_id not found"
        end
      end

      Wrapper.read_wrapper(
        :req_destination  => @req_destination,
        :bid_destination  => @bid_destination,
        :won_destination  => @won_destination
      )

    end

    def get_company_name(entity_id)
      doc = Nokogiri::XML(open(@url))
      @logger.info "Getting company name"
      @company_name = doc.at_xpath('entity')[:name]
      @company_name.gsub!(/\./, '') && @company_name.gsub!(/ +/,'_') && @company_name.downcase!
      company_info = "#{entity_id},#{@company_name}\n"
      @logger.info "Company information: #{company_info}"
      update_matrix(company_info)
    end

    def update_matrix(company_info)
      File.open(@company_matrix, "a") { |f| f.puts company_info }
      get_requested_csv
    end

    def get_requested_csv
      @logger.info "Downloading: #{@company_name}_requested.csv"
      `wget #{@url_requested} -P #{@req_destination}`
      get_bid_csv
    end

    def get_bid_csv
      @logger.info "Downloading: #{@company_name}_bid.csv"
      `wget #{@url_requested} -P #{@bid_destination}`
      get_won_csv
    end

    def get_won_csv
      @logger.info "Downloading: #{@company_name}_won.csv"
      `wget #{@url_requested} -P #{@won_destination}`
      @logger.info "All files downloaded for company: #{@company_name}"
    end
  end
end

module Wrapper
  def self.scrape_wrapper(args)
    CEUDataScraper::Scraper.new(args[:base_url],       
                                args[:req_destination],
                                args[:bid_destination],
                                args[:won_destination],
                                args[:company_matrix]
                                ).detect_redirections
  end

  def self.read_wrapper(args)
    CEUDataScraper::FolderReader.new( args[:req_destination],
                                      args[:bid_destination],
                                      args[:won_destination]
                                      ).list_requested
  end

  def self.fill_wrapper(args)
    CEUDataScraper::CSVFiller.new(args[:requested_files],
                                  args[:bid_files],
                                  args[:won_files]
                                  ).collect_requested
  end
end

Wrapper.scrape_wrapper(
  :base_url         => "http://kozbeszerzes.ceu.hu",
  :req_destination  => "/home/buub/Scriptland/Scrapes/CEU_Kozbeszerzes/csv_files/requested_copy/",
  :bid_destination  => "/home/buub/Scriptland/Scrapes/CEU_Kozbeszerzes/csv_files/bid_copy/",
  :won_destination  => "/home/buub/Scriptland/Scrapes/CEU_Kozbeszerzes/csv_files/won_copy/",
  :company_matrix   => "/home/buub/Scriptland/Scrapes/CEU_Kozbeszerzes/company_matrix_copy.csv"
)

