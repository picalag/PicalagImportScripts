###############################################################################
# START HERE: Tutorial 3: More advanced scraping. Shows how to follow 'next'
# links from page to page: use functions, so you can call the same code
# repeatedly. SCROLL TO THE BOTTOM TO SEE THE START OF THE SCRAPER.
###############################################################################

require 'rubygems'
require 'nokogiri'
require 'open-uri'
require 'date'
require 'thread'
require 'net/http'
require 'uri'
require 'fileutils'

BASE_URL = 'http://www.citylife.co.uk/whats_on/'
NB_THREADS_MAX = 6

@date=Date.today.strftime("%d/%m/%Y")
@date_dt = Date.today

if(ARGV.length == 3)
  @date_dt = Date.civil(ARGV[0].to_i,ARGV[1].to_i,ARGV[2].to_i)
  @date = @date_dt.strftime("%d/%m/%Y")
end
@day = @date_dt.day
@month = @date_dt.month
@year = @date_dt.year

puts "Scraping event for " << @date << "\n"

puts "Clean cache from previous days:\n"

Dir.foreach("cache/") { |filename|
  del = false;
  begin
    filename_to_i = Integer(filename)
  rescue ArgumentError
    filename_to_i = 99999999
  end
  if((File.directory?("cache/" << filename)) && (filename_to_i < Date.today.strftime("%Y%m%d").to_i))
    del = true
  end
  if (del)
    FileUtils.rm_rf("cache/" << filename)
    puts "cache/" << filename << "\t deleted\n"
  end
}
puts "done\n"

# define the order our columns are displayed in the datastore
#mdc = SW_MetadataClient.new
#mdc.save('data_columns', ['Id','Id_Event', 'Link', 'Title', 'LinkVenue', 'Venue', 'Category','Description','Latitude','Longitude','VenueInfo','Address','Tel','Date','Start','End'])
@i=0


# cache function
def get_url_or_cache(url)
  # create cache directory with date if not exists
  
  date=@date_dt.strftime("%Y%m%d")
 
  Dir.mkdir("cache/" << date) unless File.directory?("cache/" << date)


  # check if file is already cached, if not, add it to cache

  unless File::exist?("cache/" << date << "/" << url.gsub(/\//,'_-_').gsub(/:/,"").gsub(".","_") << ".txt")
    begin
      page = open(url)

      my_file = File.open("cache/" << date << "/" << url.gsub(/\//,'_-_').gsub(/:/,"").gsub(".","_") << ".txt", "a")
      page.each_line { |line|
        my_file << line << "\n"
      }
      my_file.close
      # puts url << " cached\n"
    rescue Timeout::Error, Errno::ECONNABORTED
      puts 'Timeout was detected while accessing ' << url << '.  Trying again...'
      retry
    end
  end

  page = File.open("cache/" << date << "/" << url.gsub(/\//,'_-_').gsub(/:/,"").gsub(".","_") << ".txt")

  return Nokogiri::HTML(page)
end

# scrape_event scrapes the event page
def scrape_event(url)
  description=""
  page = get_url_or_cache(url)
  description=page.css('p#bodytext').inner_text
  description+=' '+page.css('div#listing-details').inner_text.gsub('Details','')
  tmp=description.split
  description=""
  tmp.each do |w|
    description+=w+' '
  end
  #linkVenue=page.css('div#listingheader p.nextevent a')[0]['href']
  #puts description
  yield description
end

# scrape_venue scrapes venue info page
def scrape_venue(url,date,eventId)
  latitude=longitude=address=tel=venueInfo=starts=ends=""
  page = get_url_or_cache(url)
  #puts 'name : '+page.css('div#listingheader h2')[0].inner_text
  scriptMap = page.css('div.venue-map script').inner_text
  scriptMapArray = scriptMap.split(/\n/)
  scriptMapArray.each {|line|
    line=line.split(/"/)
    #puts "latitude : "+line[3] if line[1]=='latitude'
    latitude=line[3] if line[1]=='latitude'
    #puts "longitude : "+line[3] if line[1]=='longitude'
    longitude=line[3] if line[1]=='longitude'
  }
  details = page.css('div#listing-details li').each { |detail|
    detailSplit = detail.inner_text.split(/:/)
    detail=detail.inner_text
    detail.strip!
    detailSplit[0].strip!
    if(detailSplit[0]=='Address')
      #puts "Venue address: "+detail.gsub('Address: ','')
      address=detail.gsub('Address: ','')
    elsif(detailSplit[0]=='Tel')
      #puts "Venue phone number: "+detail.gsub('Tel: ','')
      tel=detail.gsub('Tel: ','')
    else
      #puts "Other info: "+detail
      venueInfo+=detail
    end
  }

  allEvents = page.css('table.event-table tr').each {|rowEvent|
    if((rowEvent.css('td')[0].inner_text == date) && (rowEvent.css('td')[3].css('a')[0]['href'].include? eventId.to_s))
      #puts "Start time: "+rowEvent.css('td')[1].inner_text.gsub('Starts ','')
      starts=rowEvent.css('td')[1].inner_text.gsub('Starts ','')
      #puts "End time: "+rowEvent.css('td')[2].inner_text.gsub('Ends ','')
      ends=rowEvent.css('td')[2].inner_text.gsub('Ends ','')
      break
    end
  }

  yield latitude,longitude,address,tel,venueInfo,starts,ends
end

# scrape_table scrapes the search result
def scrape_table(page)
  event_list = page.css('div.listing-search-result').each do |row|
    # new record, initialize to empty
    record = {}

    #record['Longitude'] = record['Start'] = record['Title'] = record['Id_event'] = record['Link'] = record['End'] = record['Tel'] = record['Latitude'] = record['Id'] = record['Venue'] = record['Address'] = record['Description'] = record['VenueInfo'] = record['Category'] = ""

    record['Link']    = row.css('div.secondary-info a')[0]['href']
    record['Id_event'] = record['Link'][16..20]
    if !record['Id_event'].to_s.empty?
      record['Title']     = row.css('div.secondary-info a')[0].inner_text
      if(!row.css('div.secondary-info span.venue a').empty?)
        record['LinkVenue'] = row.css('div.secondary-info span.venue a')[0]['href']
        record['Venue'] = row.css('div.secondary-info span.venue a')[0].inner_text
      else
        record['Venue'] = "Multiple Venues"
      end
      record['Category'] = row.css('span.category')[0].inner_text
      record['Id']=@i
      @i+=1

      thr_event=Thread.new{
        scrape_event('http://www.citylife.co.uk'+record['Link']) { |description|
          record['Description']=description
        }
      }

      thr_venue=Thread.new{
        if(record['Venue']!='Multiple Venues')
          scrape_venue('http://www.citylife.co.uk'+record['LinkVenue'],@date,record['Id_event']) { |latitude,longitude,address,tel,venueInfo,starts,ends|
            record['Latitude']=latitude
            record['Longitude']=longitude
            record['Address']=address
            record['Tel']=tel
            record['VenueInfo']=venueInfo
            record['Start']=starts
            record['End']=ends
          }
        end
      }

      #puts record
      # Finally, save the record to the datastore - 'Artist' is our unique key

      thr_event.join
      thr_venue.join

      #save_to_file(record)
      save_to_calagator(record)
      
    end
  end
end

#        scrape_and_look_for_next_link(starting_url)

@thr_pages=[]

## scrape_and_look_for_next_link function: calls the scrape_table
# function, then hunts for a 'next' link: if one is found, calls itself again
def scrape_and_look_for_next_link(url)
  puts url
  page = Nokogiri::HTML(open(url))
  @thr_pages << Thread.new{
    scrape_table(page)
  }
  next_link = page.css('a.pager').each do |pager|
    if pager.inner_text.include?  'Next'
      #puts pager
      next_url = pager['href']
      #puts next_url
      #puts 'NEXT PAGE: Waiting 60 secs to avoid IP banning'
      #sleep(60)

      nb_thr_alive = 0
      @thr_pages.each do |thr_page|
        if (thr_page.alive?)
          nb_thr_alive+=1
        end
      end
      if(nb_thr_alive>NB_THREADS_MAX)
        puts 'too many threads, wait for the last one to exit'
        @thr_pages.last.join
      end

      begin
        scrape_and_look_for_next_link(next_url)
      rescue Timeout::Error, Errno::ECONNABORTED
        puts 'Timeout was detected.  Trying again...'
        retry
      end
    end
  end
end

def save_to_file(record={})

  my_file = File.open("test"+@date_dt.strftime("%Y%m%d")+".txt", "a")

  record.each do |key,value|
    my_file << "#{key} = #{value}\n"
  end
  my_file << "=====================\n"

  my_file.close

end

def save_to_calagator(record={})
  #puts record
  #  if(record['LinkVenue'].nil? or record['Start'].nil?)
  #    record.each { |key,value|
  #      puts key + "=" + value
  #
  #    }
  #  end
  record['Link'] = record['Link'] << "_" << @date_dt.strftime("%Y%m%d")
  record['Link']='http://www.citylife.co.uk' << record['Link']
  record['LinkVenue']='http://www.citylife.co.uk' << record['LinkVenue'] unless record['LinkVenue'].nil?
  
  record['Start']="00:00" if record['Start'].nil? || record['Start'].empty?
  record['End']="23:59" if record['End'].nil? || record['End'].empty?

  start_split=record['Start'].split(/:/)
  start_h=""
  start_m=""
  if start_split.length == 2
    start_h=start_split[0].to_i
    start_m=start_split[1].to_i
  end
  start_date = DateTime.new(@year,@month,@day,start_h,start_m)
  record['Start']=start_date.strftime("%Y-%m-%d %H:%M")
  
  
  end_split=record['End'].split(/:/)
  end_h=""
  end_m=""
  if end_split.length == 2
    end_h=end_split[0].to_i
    end_m=end_split[1].to_i
  end
  end_date = DateTime.new(@year,@month,@day,end_h,end_m)
  if(end_date < start_date)
    end_date = end_date + 1
  end
  record['End']=end_date.strftime("%Y-%m-%d %H:%M")
  
  # save_to_file(record)
  
  begin
      res = Net::HTTP.post_form(URI.parse('http://ar-a.cs.man.ac.uk/sources/API_import_event.xml'),record)

      case res
      when Net::HTTPSuccess
      else
        puts "error " << res.to_s
        record.each {|key,value|
          puts key + "=" + value
        }
      end
    rescue Errno::ECONNREFUSED => e
      # no connection with the server
      puts "error " << e.to_s
    end

end

# ---------------------------------------------------------------------------
# START HERE - define your starting URL - then
# call a function to scrape the first page in the series.
# ---------------------------------------------------------------------------
starting_url = BASE_URL << "?date=" << @date_dt.strftime("%d-%m-%Y")
scrape_and_look_for_next_link(starting_url)

puts 'wait for all threads to end'
@thr_pages.last.join

nb_thr_alive = 0
@thr_pages.each do |thr_page|
  if (thr_page.alive?)
    nb_thr_alive+=1
  end
end
while(nb_thr_alive!=0)
  @thr_pages.last.join

  @thr_pages.each {|thr_current|
    thr_current.join
  }
  
  nb_thr_alive = 0
  @thr_pages.each do |thr_page|
    if (thr_page.alive?)
      nb_thr_alive+=1
    end
  end
end

puts 'ok'
puts "#{@i} records"