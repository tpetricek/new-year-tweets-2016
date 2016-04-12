#I @"packages/FSharp.Data.Toolbox.Twitter/lib/net40"
#I @"packages/FSharp.Data/lib/net40"
#r "packages/Suave/lib/net40/Suave.dll"
#r "FSharp.Data.Toolbox.Twitter.dll"
#r "FSharp.Data.dll"

#load "async.fs"
#load "config.fsx"

open System
open System.Web
open System.Text
open System.Collections.Generic
open FSharp.Data
open FSharp.Data.Toolbox.Twitter
open AsyncHelpers

open Suave
open Suave.Web
open Suave.Http
open Suave.Filters
open Suave.Operators
open Suave.Sockets
open Suave.Sockets.Control
open Suave.Sockets.AsyncSocket
open Suave.WebSocket
open Suave.Utils

// --------------------------------------------------------------------------------------
// Parsing time zone information - we can only display one time zone per country. We 
// get time zone info from Wikipedia & do some manual post-processing to fix the 
// zones where country names do not exactly match what the map expects...
// --------------------------------------------------------------------------------------

// Cached version of: https://en.wikipedia.org/wiki/List_of_time_zones_by_country
type TimeZones = HtmlProvider<"data/List_of_time_zones_by_country.html">
let reg = System.Text.RegularExpressions.Regex("""UTC([\+\-][0-9][0-9]\:[0-9][0-9])?""")

let explicitZones = 
  [ "France", "UTC+01:00"; "United Kingdom", "UTC"; "Kingdom of Denmark", "UTC+01:00"
    "Netherlands", "UTC+01:00"; "Portugal", "UTC"; "Spain", "UTC"; 
    "Northern Cyprus", "UTC+02:00"; "Denmark", "UTC+01:00"; "Greenland", "UTC-03:00"
    "North Korea", "UTC+08:30"; "South Korea", "UTC+09:00"; "New Caledonia", "UTC+11:00"
    "Somaliland", "UTC+03:00"; "Republic of Serbia", "UTC+01:00"
    "United Republic of Tanzania", "UTC+03:00"; "United States of America", "UTC-06:00" ]

/// Returns a list of pairs with country name and its time zone
let timeZones = 
 [| let special = dict explicitZones 
    for k, v in explicitZones do
      yield k, v
    for r in TimeZones.GetSample().Tables.Table1.Rows do
      if not (special.ContainsKey r.Country) then 
        let tz = r.``Time Zone``.Replace("−", "-")
        let matches = reg.Matches(tz)
        if matches.Count > 0 then
          yield r.Country, matches.[matches.Count/2].Value |] 

/// Returns a list of time zones, sorted from -7:00 to +13:00
let zoneSequence = 
  timeZones |> Seq.map snd |> Seq.distinct |> Seq.sortBy (fun s -> 
    match s.Substring(3).Split(':') with
    | [| h; m |] -> int h, int m
    | _ -> 0, 0 )


// --------------------------------------------------------------------------------------
// Geolocating tweets - we use MapQuest and Bing to geolocate the users based on the
// string they put in their profile (which is a guess, but better than nothing)
// --------------------------------------------------------------------------------------

module MapQuest = 
  // Use JSON provider to get a type for calling the API
  let [<Literal>] MapQuestSample = 
    "http://www.mapquestapi.com/geocoding/v1/address?location=Prague&key=" + Config.MapQuestKey
  type MapQuest = JsonProvider<MapQuestSample>

  /// Returns a tuple with the original location, geolocation API used, inferred location and lat/lng coordinates
  let locate (place:string) = async {  
    try
      let url = 
        "http://www.mapquestapi.com/geocoding/v1/address?key=" +
          Config.MapQuestKey + "&location=" + (HttpUtility.UrlEncode place)
      let! mapQuest = MapQuest.AsyncLoad(url)
      return 
        mapQuest.Results
        |> Seq.choose (fun loc ->
            if loc.Locations.Length > 0 then Some(loc, loc.Locations.[0])
            else None)
        |> Seq.tryFind (fun _ -> true)
        |> Option.map (fun (info, loc) ->
            let area = 
              [for i in 5 .. -1 .. 1 -> loc.JsonValue.TryGetProperty("adminArea" + string i) ]
              |> Seq.choose id
              |> Seq.map (fun v -> v.AsString())
              |> Seq.filter (fun s -> System.String.IsNullOrEmpty(s) |> not)
              |> String.concat ", "
            info.ProvidedLocation.Location, "MapQuest", area, (loc.LatLng.Lat, loc.LatLng.Lng))
    with e -> 
      printfn "[ERROR] MapQuest failed: %A" e
      return None }


module Bing = 
  // Use JSON provider to get a type for calling the API
  let [<Literal>] BingSample = 
    "http://dev.virtualearth.net/REST/v1/Locations?query=Prague&includeNeighborhood=1&maxResults=5&key=" + Config.BingKey
  type Bing = JsonProvider<BingSample>

  /// Returns a tuple with the original location, geolocation API used, inferred location and lat/lng coordinates
  let locate (place:string) = async {
    try
      let url = 
        "http://dev.virtualearth.net/REST/v1/Locations?query=" + 
          (HttpUtility.UrlEncode place) + "&includeNeighborhood=1&maxResults=5&key=" + Config.BingKey
      let! bing = Bing.AsyncLoad(url)
      return
        bing.ResourceSets
        |> Seq.collect (fun r -> r.Resources)
        |> Seq.choose (fun r -> 
            match r.Point.Coordinates with
            | [| lat; lng |] -> Some(place, "Bing", r.Name, (lat, lng))
            | _ -> None)
        |> Seq.tryFind (fun _ -> true)
    with e -> 
      printfn "[ERROR] Bing failed: %A" e
      return None }    


// --------------------------------------------------------------------------------------
// Searching for Happy New Year tweets & parsing tweets from Twitter
// --------------------------------------------------------------------------------------

/// Information we collect about tweets. The `Inferred` fields are calculated later 
/// by geolocating the user, all other information is filled when tweet is received
type Tweet = 
  { Tweeted : DateTime
    Text : string
    OriginalArea : string
    UserName : string
    UserScreenName : string
    PictureUrl : string 
    OriginalLocation : option<decimal * decimal>
    Phrase : int
    IsRetweet : bool
    // Filled later by geolocating the tweet
    GeoLocationSource : string
    InferredArea : option<string>
    InferredLocation : option<decimal * decimal> }
      
// Connect to twitter using the application access key (directly) & search for tweets!
let ctx = 
  { ConsumerKey = Config.TwitterKey; ConsumerSecret = Config.TwitterSecret; 
     AccessToken = Config.TwitterAccessToken; AccessSecret = Config.TwitterAccessSecret } 
let twitter = Twitter(UserContext(ctx))

/// New Year Tweets from: http://twitter.github.io/interactive/newyear2014/
let newYearPhrases = 
 ["새해 복 많이 받으세요"; "あけまして　おめでとう　ございます"; "新年快乐"; "manigong bagong taon"
  "Срећна Нова година"; "честита нова година"; "selamat tahun baru"; "สวัสดีปีใหม่"; "sugeng warsa enggal"
  "С Новым Годом"; "šťastný nový rok"; "عام سعيد"; "Щасливого Нового Року"; "שנה טובה"
  "yeni yılınız kutlu olsun"; "feliz año nuevo"; "happy new year"; "Καλή Χρονιά";"godt nyttår"
  "bon any nou"; "felice anno nuovo"; "sretna nova godina"; "godt nytår"; "gelukkig nieuwjaar"
  "Frohes neues Jahr"; "urte berri on"; "bonne année"; "boldog új évet"; "gott nytt år"
  "szczęśliwego nowego roku"; "blwyddyn newydd dda"; "feliz ano novo" 

  "سنة_جديدة_سعيدة#"; "#হ্যাপিনিউইয়ার"; "#新年快乐"; "#新年快樂"; "#StastnyNovyRok"; "#GodtNytår"
  "#GelukkigNieuwJaar"; "#HappyNewYear"; "سال‌نومبارک#"; "#OnnellistaUuttaVuotta"; "#BonneAnnée"
  "#FrohesNeuesJahr"; "#KaliXronia"; "#હેપીન્યૂયર"; "שנהאזרחיתטובה#"; "#नववर्षकीशुभकामनाएँ"; "#BoldogÚjÉvet"
  "#SelamatTahunBaru"; "#FeliceAnnoNuovo"; "#ಹ್ಯಾಪಿನ್ಯೂಇಯರ್"; "#SelamatTahunBaru"; "#ഹാപ്പിന്യൂഇയർ"
  "#हैप्पीन्यूयर"; "#GodtNyttÅr"; "#SzczęśliwegoNowegoRoku"; "#FelizAnoNovo"; "#ਹੈਪੀਨਿਊਯੀਅਰ";
  "#LaMulțiAni"; "#СНовымГодом"; "#FelizAñoNuevo"; "#GottNyttÅr"; "#ManigongBagongTaon";
  "#ஹேப்பிநியூஇயர்"; "#สวัสดีปีใหม่"; "#MutluYıllar"; "#ЗНовимРоком"; "نیا_سال_مبارک#" ]
  |> List.map (fun s -> s.ToLower())

/// Alternatively, we can just get trending phrases for a region 
let trendingPhrases region = 
  [ let world = twitter.Trends.Available() |> Seq.find (fun t -> t.Name = region)
    let trends = twitter.Trends.Place(world.Woeid)
    for t in trends.[0].Trends do yield t.Name ]

let phrases =
  match Config.TrendingRegion with
  | None -> newYearPhrases
  | Some region -> trendingPhrases region

let search = twitter.Streaming.FilterTweets phrases 

// Returns an observable of live parsed tweets with minimal filtering
let liveTweets = 
  search.TweetReceived
  |> Observable.choose (fun status -> 
      // Parse the location, if the tweet has it...
      let origLocation = 
        match status.Geo with
        | Some(geo) when geo.Type = "Point" && geo.Coordinates.Length = 2 -> 
            Some(geo.Coordinates.[0], geo.Coordinates.[1])
        | _ -> None

      // Get user name, text of the tweet and location
      match status.User, status.Text with
      | Some user, Some text ->
          let phrase = phrases |> Seq.tryFindIndex (fun phrase -> 
            text.ToLower().Contains(phrase))
          match user.Location.String, phrase with
          | Some userLocation, Some phrase ->
              let rt = text.StartsWith("RT") || status.Retweeted = Some true
              { Tweeted = DateTime.UtcNow; OriginalArea = userLocation; Text = text
                PictureUrl = user.ProfileImageUrl; UserScreenName = user.ScreenName 
                OriginalLocation = origLocation; UserName = user.Name; Phrase = phrase
                InferredArea = None; InferredLocation = None; 
                IsRetweet = rt; GeoLocationSource = "N/A" } |> Some
          | _ -> None 
      | _ -> None )

// Start the Twitter search 
//search.Start()

let s = liveTweets.Subscribe(printfn "%A")
s.Dispose()
// let liveTweets = (new Event<Tweet>()).Publish

// --------------------------------------------------------------------------------------
// Calculate frequencies of individual phrases - how many times did 
// a phrase appear among the last 100 tweeets? 
// --------------------------------------------------------------------------------------

/// Returns an observable of lists containing key value pairs with the 
/// text of the phrase and the number of times it was mentioned
let phraseCounts = 
  liveTweets 
  |> Observable.map (fun tw -> tw.Phrase)
  |> Observable.limitRate 50
  |> Observable.aggregateOver 200 -1 (fun buffer ->
      let counts = Seq.countBy id buffer |> Seq.filter (fun (k, v) -> k <> -1) |> dict
      phrases |> List.mapi (fun i p -> 
        match counts.TryGetValue(i) with
        | true, r -> p, r
        | _ -> p, 0))

// --------------------------------------------------------------------------------------
// Tweets on the map - to put tweets on the map, we need a location. We get it either
// from the tweet itself, or by geolocating the user's location (but this we can do
// only with a limited frequency), or by a wild guess based on the phrase!
// --------------------------------------------------------------------------------------

let replay = SchedulerAgent<Tweet>()
let geoLocated = Event<Tweet>() 
let alt = AlternativeSourceAgent<Tweet>(3000)

replay.ErrorOccurred.Add(printfn "[ERROR] Replay agent failed %A")
alt.ErrorOccurred.Add(printfn "[ERROR] Alt agent failed %A")

// Handle tweets that come with a geolocation information (yay!)
liveTweets
|> Observable.choose (fun tw ->
    match tw.OriginalLocation with
    | Some loc -> 
        ( tw.Tweeted.AddSeconds(5.0),
          { tw with GeoLocationSource = "Twitter"; InferredLocation = Some loc; 
                    InferredArea = Some tw.OriginalArea }) |> Some
    | _ -> None)
|> Observable.addWithError 
    (fun tw ->
      geoLocated.Trigger(snd tw)
      replay.AddEvent(tw) )
    (printfn "[ERROR] GPS live tweets failed: %A")


// To show more tweets on the map when not enought location data is available, we
// remember recent locations for each phrase and show tweets with the same phrase
// in some of the previous locations.
let locations = KeepStateAgent<Tweet, Map<int, list<decimal * decimal>>>()

// The following remembers the past locations and sends them as state to `locations`
geoLocated.Publish
|> Observable.scan (fun (locs:RingBuffer<_>[]) tw ->
    locs.[tw.Phrase].Add(tw.InferredLocation)
    locs) (phrases |> Array.ofSeq |> Array.map (fun _ -> RingBuffer(500, None)))
|> Observable.map (fun locs ->
    locs 
    |> Seq.mapi (fun i buf -> i, buf.Values |> List.choose id)
    |> Seq.filter (fun (i, buf) -> not (List.isEmpty buf))
    |> Map.ofSeq )
|> Observable.addWithError (locations.SetState) (printfn "[ERROR] Geolocated tweets failed: %A")

// Annotate the live tweets with the current state
liveTweets |> Observable.add (locations.AddEvent)

// "Geo-locate" the tweet and post it to the alternative source which is only
// used when not enough good data is available
locations.EventOccurred
|> Observable.addWithError(fun (map, tw) -> 
    map.TryFind tw.Phrase |> Option.iter (fun locations ->
      let loc = locations.[Random().Next(locations.Length)]
      let tw = { tw with GeoLocationSource = "Random"; InferredLocation = Some loc; InferredArea = Some tw.OriginalArea  }
      alt.AddEvent(tw) ))
    (printfn "[ERROR] Locations tweets failed: %A")

// Replay events from the alternative source
alt.EventOccurred 
|> Observable.addWithError (fun tw -> replay.AddEvent(tw.Tweeted.AddSeconds(10.0), tw))
    (printfn "[ERROR] Alt tweets failed: %A")


// Geolocating tweets using MapQuest or Bing occasionally...
// Add index in range 0 .. 1 to each tweet (for choosing Bing or MapQuest)
let indexedTweets =
  liveTweets
  |> Observable.stateful 0 (fun n tw -> (n+1)%2, tw)

let startGeoLocating i rate f = 
  indexedTweets 
  |> Observable.filter (fun (n, _) -> n = i)
  |> Observable.filter (fun (_, tw) -> tw.OriginalArea |> Seq.exists Char.IsLetter)
  |> Observable.limitRate rate
  |> Observable.mapAsyncIgnoreErrors (fun (_, tw) -> async {
      let! located = f tw.OriginalArea
      return located |> Option.map (fun (_, api, area, loc) ->
        tw.Tweeted.AddSeconds(10.0),
        { tw with GeoLocationSource = api; InferredLocation = Some loc; InferredArea = Some area }) })
  |> Observable.choose id
  |> Observable.addWithError (fun tw ->
      alt.Ping() 
      geoLocated.Trigger(snd tw)
      replay.AddEvent(tw) )
    (printfn "[ERROR] Geolocated using %d tweets failed: %A" i)

startGeoLocating 0 5000 Bing.locate
startGeoLocating 1 6000 MapQuest.locate


// --------------------------------------------------------------------------------------
// Web server - serves static files from the `web` directory and exposes
// API end-point `/zones` for getting time zone info. Most importantly, provides
// 3 web socket end-points for reporting tweets (on map & in feed) and frequencies
// --------------------------------------------------------------------------------------

/// Use JSON type provider to generate types for the things we are returning
type JsonTypes = JsonProvider<"""{
    "socketFeedTweet":
      { "text":"hello", "originalArea":"World", 
        "picture":"http://blah.jpg", "name":"sillyjoe", "screenName": "Silly Joe" },
    "socketMapTweet": 
      { "latitude":50.07, "longitude":78.43, "geocoder": "Random", "text":"hello", "originalArea":"World", "inferredArea":"World",
        "picture":"http://blah.jpg", "name":"sillyjoe", "screenName": "Silly Joe" },
    "socketPhrase": 
      [ {"phrase":"Happy new year", "count":10}, { "phrase":"manigong bagong taon", "count":5 } ],
    "timeZoneInfo": 
      { "countries": [ {"country": "UK", "zone": "UTC" }, {"country": "UK", "zone": "UTC" } ],
        "zones": [ "UTC", "UTC+00:00" ] }
  }""">


/// Tweets with location information for the map
let mapTweets = 
  replay.EventOccurred
  |> Observable.map (fun tweet ->
      JsonTypes.SocketMapTweet(
        fst tweet.InferredLocation.Value, snd tweet.InferredLocation.Value, tweet.GeoLocationSource, tweet.Text, 
        tweet.OriginalArea, tweet.InferredArea.Value, tweet.PictureUrl, tweet.UserName, tweet.UserScreenName).JsonValue.ToString())

/// Tweets possibly without location information for the feed
let _, feedTweets = 
  liveTweets
  |> Observable.filter (fun tw -> not tw.IsRetweet)
  |> Observable.limitRate 850
  |> Observable.map (fun tweet ->
      JsonTypes.SocketFeedTweet(
        tweet.Text, tweet.OriginalArea, tweet.PictureUrl, tweet.UserName, tweet.UserScreenName).JsonValue.ToString())
  |> Observable.start

/// Phrase frequencies for the word cloud
let _, phraseUpdates = 
  phraseCounts 
  |> Observable.map (fun counts ->
      JsonValue.Array([| for p, c in counts -> JsonTypes.SocketPhrase(p, c).JsonValue |]).ToString() )
  |> Observable.start

/// Passes updates from IObservable<string> to a socket
let socketOfObservable (updates:IObservable<string>) (webSocket:WebSocket) cx = socket {
  while true do
    let! update = updates |> Async.AwaitObservable |> Suave.Sockets.SocketOp.ofAsync
    do! webSocket.send Text (Encoding.UTF8.GetBytes update) true }

/// Time-zone information (calcualted just once) packaged as a JSON
let timeZonesJson = 
  (JsonTypes.TimeZoneInfo
    (Array.map (fun (c, z) -> JsonTypes.Country(c, z)) timeZones, 
     Array.ofSeq zoneSequence)).ToString()
  
// Compose & start the web server!
let part =
  let root = IO.Path.Combine(__SOURCE_DIRECTORY__, "web")
  choose 
    [ 
      path "/maptweets" >=> handShake (socketOfObservable mapTweets)
      path "/feedtweets" >=> handShake (socketOfObservable feedTweets)
      path "/frequencies" >=> handShake (socketOfObservable phraseUpdates)
      path "/zones" >=> Successful.OK timeZonesJson
      path "/" >=> Files.browseFile root "index.html" 
      Files.browse root ]

// Start the server using the IP & port specified in the config
let config = 
  { defaultConfig with 
      logger = Logging.Loggers.saneDefaultsFor Logging.LogLevel.Verbose
      bindings = [ HttpBinding.mkSimple Protocol.HTTP Config.IP Config.Port ] }
let start, run = startWebServerAsync config part
let ct = new System.Threading.CancellationTokenSource()
Async.Start(run, ct.Token)
