const request     = require('request');
const cheerio     = require('cheerio');
const async       = require('async');
const MongoClient = require('mongodb').MongoClient;

require('dotenv').config()

const baseUrl     = "https://www.eia.gov/dnav/pet";
const mainUrl     = baseUrl + "/pet_pri_spt_s1_d.htm";
const product     = "Ultra-Low-Sulfur No. 2 Diesel Fuel";
const records     = {};

// Connection URL
const url         = process.env.DB;
const dbName      = 'heroku_dnzhkpq8';
const collection  = 'usa-diesel-spot-price';
let client;

console.log('Connecting to Database ...');
MongoClient.connect(url, { useNewUrlParser: true, useUnifiedTopology: true }, (err, _client) =>{
  if(err) return handelError(err, "Mongo Client Connection");
  console.log('Database Connected!');
  client = _client;
  // getDataFromMainUrl();
});

function getDataFromMainUrl(){
  console.log('Fetching Main Url ...');
  request(mainUrl, (error, response, body) => {
    if (error) return handelError(error, "Get Data From Main Url");

    const $ = cheerio.load(body);
    let indices = [], // to store the indices of areas under a product
        areas = [], // to store the area name and reference url
        storeIndices = false;

    // find the product index and the related areas indices
    $('.DataRow').filter((i, el)=>{
      let title = $(el).children('.DataStub2')
                       .children('.data2')
                       .children('tbody')
                       .children('tr')
                       .children('.DataStub2')
                       .text()
                       .trim();

      if(storeIndices){
        if(!title) indices.push(i);
        else storeIndices = false;
      }
      if (title == product) {
        storeIndices = true;
      }
    });

    // find area names and reference url
    for (var i = 0; i < indices.length; i++) {
      areas.push({
        name: $('.DataRow').eq(indices[i])
                           .children('.DataStub')
                           .children('.data2')
                           .children('tbody')
                           .children('tr')
                           .children('.DataStub1')
                           .text()
                           .trim(),
        url: baseUrl + $('.DataRow').eq(indices[i])
                          .children('.DataHist')
                          .children('a')
                          .attr('href').substr(1)
      })
    }

    console.log(`${areas.length} Areas Found!`);
    crawlAreas(areas);
  });
}

function crawlAreas(areas) {
  console.log('Fetching Each Area ...');
  async.each(areas,
    (area, callback)=>{
      getAreaData(area, callback);
    },
    (err)=>{
      if (err) return handelError(err, "Get Area Data");

      console.log(`All Area Fetched! ${Object.keys(records).length} Records Found!`);
      let data = Object.entries(records).map((e) => {return ({ date: e[0], record: e[1] })});
      storeData(data);
    });
}

function getAreaData(area, callback){
  request({ url: area.url, followRedirect: true }, (error, response, body) => {
    if (error) return callback(error);

    const $ = cheerio.load(body);

    $("table[summary*='"+area.name+"']").children('tbody').children('tr').each((i, el)=>{
      if(i > 0 && $(el).children('td').eq(0).text().trim()){
        let firstDate = formatDate($(el).children('td').eq(0).text().trim().split(' to')[0]);
        $(el).children('td').each((j, elm)=>{
          if(j > 0){
            let _date = new Date(firstDate);
                _date = new Date(_date.setDate(_date.getDate() + (j-1))).toISOString();

            if(!records[_date]) records[_date] = [];
            records[_date].push({ name: area.name, price: toFloat($(elm).text().trim()) })
          }
        })
      }
    });
    callback();
  });
}

function storeData(data){
  console.log(`Storeing Records ...`);
  client.db(dbName).collection(collection).insertMany(data, (err, res)=>{
    if (err) return handelError(err, "Store Data");
    console.log(`Done! Number of documents inserted : ${res.insertedCount}`);
    client.close();
    process.exit();
  });
}

function formatDate(dateString){
  let format = {
    'Jan' : '0',
    'Feb' : '1',
    'Mar' : '2',
    'Apr' : '3',
    'May' : '4',
    'Jun' : '5',
    'Jul' : '6',
    'Aug' : '7',
    'Sep' : '8',
    'Oct' : '9',
    'Nov' : '10',
    'Dec' : '11'
};
  let data = dateString.replace('-', ' ').replace('  ', ' ').split(' ');
  return new Date(data[0], format[data[1]], data[2], 3, 0, 0, 0);
}

function toFloat(val){
    if(!val) return '';
    let y = parseFloat(val).toFixed(3);
    return parseFloat(y);
}

function handelError(error, src){
  console.log(`Error From [${src}]: `, error);
  process.exit();
}
