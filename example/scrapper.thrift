struct Blog {
  1: required string url;
  2: required string content;
}

service Scrapper {
  Blog scrape(1: string url);
}
