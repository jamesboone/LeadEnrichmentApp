import logging
import ipdb
from modules.data_cleaner import DataCleaner
from fuzzywuzzy import fuzz
logger = logging.getLogger(__name__)


class Validator(object):
    def __init__(self):
        logger.debug('validator init')

    def validate_company(self, name, fb_name, yelp=False):
        ''' Verify that a company found from fb is the company we want and not the wrong page
        '''
        logger.debug('validate_company')
        try:
            our_name = DataCleaner.name_normalizer(name)
            their_name = DataCleaner.name_normalizer(fb_name)

            try:
                fuzzy_score = fuzz.ratio(our_name, their_name)
            except UnicodeDecodeError:
                fuzzy_score = fuzz.ratio(DataCleaner.name_normalizer(name), their_name)

            if fuzzy_score < 50 and not yelp:
                states = ["alabama", "montana", "alaska", "nebraska", "arizona", "nevada", "oregon",
                          "new hampshire", "california", "new jersey", "colorado", "new mexico",
                          "connecticut", "new york", "delaware", "north carolina", "florida",
                          "north dakota", "georgia", "ohio", "hawaii", "oklahoma", "idaho",
                          "illinois", "pennsylvania", "indiana", "rhode island", "iowa", "arkansas",
                          "south carolina", "kansas", "south dakota", "kentucky", "tennessee",
                          "louisiana", "texas", "maine", "utah", "maryland", "vermont",
                          "virginia", "michigan", "washington", "minnesota", "west virginia",
                          "mississippi", "wisconsin", "missouri", "wyoming", "massachusetts"]
                for state in states:
                    if state in their_name:
                        fuzzy_score = 0
                        break
            if yelp:
                return fuzzy_score
            if fuzzy_score == 100:
                return "Perfect"
            elif fuzzy_score > 75:
                return "Good"
            elif fuzzy_score > 50:
                return "Average"
            elif fuzzy_score > 25:
                return "Below Average"
            elif fuzzy_score > 0:
                return "Bad"
            else:
                return "Wrong"
        except:
            logger.error('Validate Company Error')
            return None

    def get_category_list(self, fb_data):
        ''' TODO: normmalize category data for usefulness in salesforce, not the best place, but will
            suffice for now
        '''
        logger.debug('get_category_list')
        cat_list = ''
        if fb_data:
            for i in fb_data.get('category_list', []):
                if not cat_list:
                    cat_list = i.get('name', "").encode('utf8')

                elif i.get('name') and i['name'] not in cat_list:
                    cat_list += ', ' + i.get('name', "").encode('utf8')
        return cat_list

    def validate_lead(self, data, fb_data, rekindle=False):
        ''' Validates lead for distribution, negative filters, and bad fb categories
        '''
        logger.debug('validate_lead')

        if rekindle and not fb_data:
            return (1, "No FB Data")

        if fb_data:
            cat_list = self.get_category_list(fb_data.get('page_data'))

        neg_co_names = [
            " land & cattle",
            "1-800", "2nd & charles", "2nd and charles", "7-eleven", "76 gas", "9 round", "99 ranch market",
            "54th Street", "7 Eleven Store",
            "a&w", "abercrombie", "academy sports", "academy sports & outdoors", "adidas", "aeropostale",
            "aerosoles", "all day cafe by minervas", "alumni hall", "amc", "amerian eagle outfitters",
            "american apparel", "american eagle", "american lubefast", "american pie pizzas & salads",
            "Apple Store", "AT&T", "Aquarium Restaurant", "Apple Valley Natural", "Arby",
            "amf ", "ann taylor", "anthropologie", "applebees", "apricot lane", "arby's", "arbys",
            "ashley stewart", "associates", "athleta", "athleta", "athlete's foot", "au bon pain",
            "au bon pain", "auntie anne's", "Asian Food Market", "Altar'd State", "Adult Videos",
            "adult", "Alliance Nutrition",
            "babbage", "babies r us", "babiesrus", "banana republic", "banana republic", "bar louie",
            "barnes", "baskin-robbins", "bass pro", "bcbg", "beach bum tanning", "bebe", "bebe",
            "BURGERFI", "Burger King", "BevMo", "Biscuits Cafe", "Brunello Cucinelli",
            "Battery Systems Of Missoula", "Braums Ice Cream", "Brick House Tavern", "BURGER BRASSERIE",
            "becks prime", "belk", "bella bella", "benihana", "bennigan", "bernard zins", "best buy",
            "big and tall", "biggby coffee", "bj's restaurant & brewhouse", "black angus steakhouse",
            "black oak caf", "blockbuster", "blue ridge mountain", "bob evans restaurant",
            "bob roncker's running spot", "boot barn", "bootjack", "boston market", "bouchon bakery",
            "bp gas", "bridal", "brides", "brighton", "brokers", "brooks brothers", "brookshire",
            "bruegger's bagels", "bubba gump shrimp", "buca di beppo", "buffalo wild wings", "boyd",
            "Build-A-Bear",
            "build a bear", "build-a bear", "burberry", "burger king", "burger ranch", "burlington",
            "burlington coat factory", "buy buy baby", "Braum's Ice", "Bill Miller Barbeque",
            "Big 5 Sporting Goods", "Big Y", "Big Y World Class Market", "Bi-Lo", "Bull Moose",
            "Babaziki", "Baker's Drive Thru", "Bakers Drive Thru", "Bowl America", "Brunswick",
            "c c p fashions", "c j banks", "cabela", "cafe du monde", "california pizza kitchen",
            "Champps Restaurant", "Cheddar's", "Cheddars", "Chili's", "Chilis", "Chuck E Cheese",
            "camping world", "canine academy", "carhartt", "caribou coffee", "carrabba's italian grill",
            "carter's babies & kids", "casual male xl", "cato", "cavender's", "cavender's boot city",
            "chapelure", "charlotte russe", "cheesecake factory", "chevron", "chick-fil-a", "chik-fil-a",
            "chili's", "chili's", "chipotle", "christopher & banks", "chronic tacos", "chuck e cheese",
            "cinnabon", "citi trends", "clinic", "clothes mentor", "coach", "coach factory outlet",
            "coffee bean and tea leaf", "cole haan", "common grounds", "community coffee", "cici",
            "complete nutrition", "convention", "cooper's hawk", "corner bakery", "corp", "corporation",
            "costco", "council oak", "crab shack", "cracker barrel", "crooked goose", "Cafe Rio",
            "culinary", "culver's", "culvers", "cvs", "crumbs bake shop", "Country Inn", "consulting",
            "Converse", "College Point Multiplex Cinema", "Converse", "Chick Fil A", "Cinemark",
            "Cvs Pharmacy", "Carl's Jr", "carls Jr", "Cash Wise Foods", "Chili's Bar & Grill", "Carrabas",
            "Chilis Bar & Grill", "Chuy's", "Chuys", "Cold Stone Creamery", "Corinth Hen House Market",
            "Cosi", "Creekside Outpost", "Coyote Canyon", "Crazy Bowls",
            "dairy king", "dairy queen", "davidstea", "denny", "department store", "dick's sporting goods",
            "dillard's", "dion's", "direct tv", "disposal service", "distrib", "dog training",
            "dollar general", "dollar tree", "dollar tree", "domino", "don thomas", "dress barn",
            "duck donuts", "dunkin", "dunkin donuts", "dunn bro", "dxl", "dog training", "dressbarn",
            "Damon's Grill and Sports Bar", "Dodge City Travel Center", "Del Taco", "Dillons",
            "Dave & Buster", "Dave and Buster", "Dean & Deluca", "Dean and Deluca", "Delicious Bouquets",
            "Dynamic Bowling",
            "eb games", "eddie bauer", "edible arrangements", "einstein", "einstein bros bagels",
            "elegant eyes", "eli's on whitney", "ember room", "enterprise", "env salon",
            "Eager Beaver Car Wash", "E.A.T. Gifts", "Earth Fare", "Equinox",
            "everything but water", "express worldbrand", "extreme pita", "Eddie Merlot's",
            "fabletics", "family express", "family general", "family video", "famous footwear", "fye",
            "farm & home oil co", "farmer brothers", "fashion cents", "fatburger", "fire & ice",
            "firehouse subs", "five guys", "fleet feet", "florist", "fogo de chao", "foot locker",
            "forever 21", "fosters freeze", "four seasons", "francesca's", "francescas", "fred meyer",
            "forever21", "Flannery's Pub", "Flannerys Pub", "Floyd's 99 Barbershop", "Floyds 99 Barbershop",
            "free people", "freebirds", "friday's", "friendly's", "fuddrucker", "Frisch's Big Boy",
            "Fleming's Prime Steakhouse & Wine Bar", "Fred Astaire Dance Studios", "Flight 001",
            "Fossil Store", "Fresh & Co", "f.y.e", "Fantasy Flight Games", "Faccia Luna Pizzeria",
            "Family Thrift Center", "eXtreme Bodyshaping", "Firebirds Wood Fired Grill", "Flying Saucer",
            "Food 4 Less", "Food Lion of Greensboro", "Food Lion", "Footaction",
            "game stop", "gamestop", "gander mountain", "gap factory", "gap outlet", "garfields",
            "gigis cupcakes", "gnc", "golden corral", "good times burgers", "Gator's Dockside",
            "good times burgers & frozen custard", "goodberry's frozen custard", "goodwill", "gymboree",
            "Grand Traverse", "Guess", "Graphic Design",
            "hardee's", "harley davidson", "harley-davidson", "harris teeter", "herbalife", "hibbett sport",
            "hobby lobby", "hockeytown cafe", "hog's breath inn", "hollister", "home fitness",
            "hometown buffet", "honeybaked ham", "hooters", "horton", "hot topic", "houlihan", "Hudson's",
            "Hy-Vee Market Cafe", "Hastings", "The Habit Burger Grill", "Hastings", "Hard Rock c",
            "hughes supply", "Happy Joe's Pizza & Ice Cream Parlors", "Honor Yoga", "High School",
            "ihop", "institute", "islands restaurant", "Indy Auto Parts", "Indy?s College Bookstore",
            "It's Sugar", "IMAX", "It'sugar",
            "j crew", "j jill", "j. crew", "j.crew", "j.jill", "jos  a bank", "Jason's Deli", "Jet's Pizza",
            "jack in the box", "jamba juice", "jcpenney", "jcrew", "jersey mike's", "jerusalem bakery",
            "jewelry", "jiffy lube", "jimmy choo", "jimmy john", "jl beers", "johnny rockets",
            "jos a bank", "jos a banks", "jos. a. bank", "joseph a banks", "juicy couture", "justin boots",
            "Jasons Deli", "JFK", "Jets pizza",
            "k&g fashion superstore", "kennels", "kenneth cole", "kilwin's", "kohl's", "kohl's", "kohls",
            "krispy kreme", "krispy kreme", "kroger", "kung fu tea", "kungfu tea", "kmart",
            "Kona Grill", "Kid To Kid", "Kmart", "Kinmont", "Kate Spade", "kfc",
            "laboratories", "landry's", "lane bryant", "levi's outlet store", "lids", "limited express",
            "limited stores inc", "limited too", "little caesar", "llc", "logan's roadhouse", "LEGO",
            "longhorn steakhouse", "longhorn steakhouse", "longhorn steakhouse", "louie's grill and bar",
            "ltd", "lubrication systems co", "lucille's road house", "lucky brand", "lucky's lounge",
            "lucky's lounge", "luigis pizza", "lululemon", "Le Labo", "L'eggs Hanes Bali Outlet Store",
            "Le Pain Quotidien", "Luckys Market", "Little Caesaers", "Little Ceasars", "LKQ",
            "Loreal", "Lowes Food", "Luigi", "Luigi's Pizzeria",
            "macy's", "madewell", "marble slab", "marie callender's", "market broiler", "marshalls",
            "mary's market", "massage envy", "maurices", "max & erma's", "mc donald", "mcdonald's",
            "mcgee tire", "mcmenamins", "mellow mushroom", "men's wearhouse", "menchie's frozen yogurt",
            "menchies", "michael kors", "michaels", "midas", "mimi's cafe", "mobil gas", "morton's",
            "motherhood maternity", "mr pickles", "medical", "McAlister's Deli", "Mainstream Boutique",
            "Massage Heights Gulf Coast", "MasterCuts", "Mephisto", "Main Street Cafe", "Micro Center",
            "Microsoft Store", "Movie Tavern Horizon", "Marco's Pizza", "Manifest Discs & Tapes",
            "Mongolian Grill", "Market Basket", "Market Street", "Max & Erma", "McAlister", "Meijer",
            "Melt", "Menchie", "Mother's Nutritional Center", "Mothers Nutritional Center",
            "McDonalds", "Morton", "Mrs Greens Natural",
            "new balance", "new york & co", "new york and company", "newk's eatery", "nike", "nordstrom",
            "north face", "Nestle Toll House Cafe by Chip", "Natural Ovens Bakery", "Opry Mills",
            "Natural Organics Inc.", "Natural Grocers", "Natural Grocers By Vitamin Cottage",
            "Noodles & Company", "NEMO Equipment Inc", "New Orleans Fish House", "Noodles and Co",
            "Noodles and Company", "Nothing bundt cakes", "Noodles & Co",
            "o'charley's restaurant", "oak steakhouse", "old chicago", "old navy", "olive garden",
            "on the border", "once upon a child", "optometry", "orvis", "oscar blandi", "osteria serafina",
            "outback", "O'Charley's", "O'Charleys", "OCharleys", "Off N Running", "Olga's Kitchen",
            "Olgas Kitchen", "One Stop Market", "Obey", "Off Broadway Shoes",
            "p s s united", "p.f. chang's china bistro", "pacific plastic surgery", "panera", "papa john",
            "payless shoe source", "peet", "pei wei", "perkins restaurant", "pet co", "pet smart",
            "pet supply plus", "pet valu", "petsmart", "pharmaceutical", "philz", "pinkberry", "pizza hut",
            "pj's coffee", "plato's closet", "platos closet", "plumbing", "porter and frye", "Puma",
            "potbelly sandwich", "pet smart", "Patagonia", "Precision Tune Auto Care", "panda express",
            "Philadelphia Mills", "Pinheads", "Payson Feed & Pet Supply", "Perkins Family Restaurants",
            "Petco", "Pita Pit", "Price Chopper", "Primo Hoagies", "P.F.", "Petland", "Pharma", "Pieology",
            "Ping Golf", "Popeyes", "Portillo's Hot Dogs", "Portillos Hot Dogs", "Punch Bowl Social",
            "Qdoba",
            "rainbow apparel", "raising cane's", "rally's", "real deals on home decor", "red lobster",
            "red mango", "red robin", "red robin", "rei", "rite aid", "rosa's cafe", "ross dress for less",
            "ross store", "ruby tuesday", "ruby tuesday", "rue 21", "rue21", "running room", "runza",
            "Runnings of Hutchinson", "Ralph's Coffee", "RadioShack", "Rainbow", "Ralph Lauren", "Rip Curl",
            "Rogers Sporting Goods",
            "saks fifth avenue", "saltgrass", "salvation army", "save mart", "saxby", "schlotzsky's",
            "scrubs & beyond", "scrubs and beyond", "sears", "shakey", "shell gas", "sheplers", "shipley",
            "shoe carnival", "shoe carnival", "sitter", "skechers", "smashburger", "smoker friendly",
            "smokin joes coffee house", "smoothie king", "soma intimates", "sonic", "sonic drive in",
            "sonic drive-in", "spencer's", "sports authority", "starbuck", "starbucks",
            "stauf's coffee roasters", "stauf's coffee roasters", "stein mart", "steinmart", "steve madden",
            "subway", "summit grill and bar", "sweetgreen", "Saladworks", "Stater Bros. Markets",
            "Sahalie", "Safeway", "Sephora", "s Little Italy", "Senior Citizens", "Senior Nutrition",
            "Shoot Straight", "Shula Burger", "Stanley Food Pride", "Sun Mart Food", "SweetFrog",
            "senior center", "SmartPak",
            "t.g.i. friday's", "taco bell", "tailoring",
            "talbot", "target", "tattoo charlie's", "tea station", "teavana", "texaco", "texas roadhouse",
            "texas roadhouse", "the aveneue", "the boat house", "the buckle", "the children's place",
            "the couch tomato cafe", "the disney store", "the eagle otr", "the egg i", "the gap",
            "the iowastater restaurant", "the limited", "the macintosh", "the vans dqm general",
            "therapeutic elements center for massage therap", "tijuana flats", "tilly's", "tim horton",
            "tim horton's", "tim hortons", "tio juan's margaritas", "tj maxx", "tobacco road outlet",
            "togo's sandwiches", "tom james", "tommy hilfiger", "tony packo's", "topgolf", "torrid",
            "tory burch", "toyota", "toys r us", "toysrus", "toysrus", "tractor supply",
            "Town Pump Food Store", "The Fashion Mall at Keystone", 'Toys "r" Us/babies "r" Us',
            "Tax Services", "travelpro luggage", "turners outdoorsman", "tux", "twin peaks",
            "true religion", "true runner", "Taco Mac", "The Habit", "Thunder Bay Grill", "Tomato Bros",
            "ugg", "ugg australia", "ulta beauty", "urban outfitters", "urban outfitters",
            "urban outfitters", "Urban Roofscape", "Unchained Taphouse", "Uno Pizz", "Uno Pizzeria",
            "value village", "valvoline", "van dykes", "vans", "victoria secret", "victoria's secret",
            "victoria's secret", "village inn", "visitors", "vitamin shoppe", "volcom", "von maur",
            "von maur", "Vitamin Discount", "Vitamin World",
            "waffle house", "walmart", "walmart", "warby parker", "water st books", "wawa food markets",
            "wegmans", "wendy", "wendy's", "wendy's", "wendys", "wet seal", "whataburger", "which wich",
            "white castle", "white castle", "white house black market", "whole earth privision co",
            "whole foods market", "wild wing cafe", "willy joe's", "wingstop", "wingstreet", "wolfgang",
            "world of beer", "Wayback Burgers", "Woodhouse Day Spa",
            "yard house", "yogurtlab", "yoshinoya", "younkers", "Yogurtini", "Yogurtland",
            "yankee candle company",
            "zara", "zaxby's", "zinga frozen", "zoe's kitchen", "zumi"
            "10 spot aviation plaza", "17th street surf shop", "2nd time around",
            "2nd wind exercise equipment", "a step a head", "a step ahead",
            "34th street deli", "4 kid", "5 dollar pizza", "5 state maly", "a & e store",
            "academy sport", "ace hardware", "ada's ceramics & baskets", "adas ceramics & baskets",
            "ada's ceramics and baskets", "advanced laser & cosmetic center",
            "adas ceramics n baskets", "ada's ceramics n baskets", "adas ceramics and baskets",
            "advanced laser and cosmetic center", "advanced laser n cosmetic center",
            "aeropostale", "aerosoles", "affordables apparel", "aladdin's eatery", "aladdins eatery",
            "alice and olivia", "alice n olivia", "alice & olivia", "allen edmonds", "american spoon foods",
            "anne klein", "alessi", "athletes foot", "athletic supporter",
            "arctic circle", "aristelle", "aritzia", "ash restaurant", "athlete's foot",
            "auntie anne", "avail vapor", "avani spa", "avenue plus", "azkara", "b. & e. feed & pet supply",
            "b. & e feed & pet supply", "b & e. feed & pet supply", "b & e feed & pet supply",
            "b. & e feed n pet supply", "b & e. feed n pet supply", "b & e feed n pet supply",
            "b. & e. feed and pet supply", "b. & e. feed n pet supply", "b. n e. feed & pet supply",
            "b. & e feed and pet supply", "b & e. feed and pet supply", "b & e feed abd pet supply",
            "b. n e feed & pet supply", "b n e. feed & pet supply", "b n e feed & pet supply",
            "b. n e feed and pet supply", "b n e. feed and pet supply", "b n e feed and pet supply",
            "b. n e feed n pet supply", "b n e. feed n pet supply", "b n e feed n pet supply",
            "b. and e. feed and pet supply", "b. n e. feed and pet supply", "b. n e. feed n pet supply",
            "b. and e feed and pet supply", "b and e. feed and pet supply", "b and e feed and pet supply",
            "b. and e. feed n pet supply", "b. and e feed n pet supply", "b and e. feed n pet supply",
            "b and e feed n pet supply", "b. and e. feed & pet supply", "b. and e feed & pet supply",
            "b and e. feed & pet supply", "b and e feed & pet supply", "baby's room", "babys room",
            "baits and line", "baits n line", "baits & line", "bank jos a clothiers",
            "bank joseph a clothiers", "bachrach", "bella bridesmaid", "big apple bagels",
            "baps shayona", "barriques", "bass company store", "bass gambling", "bass shoe outlet",
            "bella ella boutique", "ben & jerry", "ben and jerry", "ben n jerry", "benetton", "bevello",
            "birkenstock", "black bear diner", "blackjack pizza", "blue sushi sake grill",
            "boltons", "bon worth", "bon-ton", "bonworth", "boston jean company", "bostonian clarks",
            "bottega venetta", "brandy melville corte madera", "bridgewater chocolate", "brighton",
            "broken yolk cafe", "bostonian hanover shoes", "bob's stores", "bobs stores",
            "brooks brothers", "brown shoe company", "brown's shoe fit", "browns shoe fit",
            "buckle for guys & gals", "buckle for guys and gals", "buckle for guys n gals",
            "buehler's fresh food", "browns shoe", "brown's shoe", "cadillac ranch", "cartridge world",
            "buehlers fresh food", "buffalo's cafe", "buffalos cafe", "burkes outlet", "burlington shoes",
            "cafe laureate restaurant", "captain d's seafood", "captain ds seafood", "carolee designs",
            "casadei", "casey's pizza and grill", "casey's pizza and grill", "caseys pizza & grill",
            "caseys pizza n grill", "casey's pizza & grill", "cash converters", "catherines plus sizes",
            "central uniforms", "casey's pizza n grill", "children's place", "childrens place",
            "chaps", "charley's grilled subs", "charleys grilled subs", "chattanooga bakery",
            "china poblano", "chipolte", "chopt creative salad", "chuck e. cheese's", "citgo gas station",
            "cj banks", "claire's boutique", "claires boutique", "clarks bostonian", "clothing emporium",
            "club monaco", "city limits diner", "cotton on", "crazy 8", "cynthia rowley", "d'angelo",
            "coco's gifts", "cocos gifts", "coldwater creek", "complete petmart", "corto olive",
            "crazy mocha coffee", "crescent moon children", "crystal lake cafe", "curry house",
            "daffin's candies", "daffins candies", "daiso", "dan's management company",
            "dans management company", "danvers", "diningin", "domino's pizza", "earthbound trading",
            "daphnes california greek", "daphne's california greek", "deli delicious", "diesel",
            "dominos pizza", "don galani", "eagle eye outfitters", "earthbound trading",
            "eastern mountain sports", "eat'n park restaurant", "eatn park restaurant",
            "eat n park restaurant", "eat & park restaurant", "edible arrangements",
            "eileen fisher boutique", "el pollo loco", "eat and park restaurant", "earthfruits yogurt",
            "elegant baby", "emack & bolio", "emack and bolio", "emack n bolio", "esther price candies & gift",
            "esther price candies n gift", "esther price candies and gift", "eureka! discover american craft",
            "express limited", "factory brand shoes", "fallas paredes", "family dollar", "fendi",
            "fleur de lis", "flowers baking company of kentucky", "footsmart", "forman mills ewing",
            "four elements cafe", "funcoland", "gambino", "five guys", "garage clothing",
            "freddy's frozen custard-steak", "freddys frozen custard-steak", "frontera mex-mex grill",
            "game x change", "games stop", "gamesstop", "gapbody", "gap body", "gapkids", "gap kids",
            "garage door broken spring replacement", "garden palace", "corporation", "giant food inc",
            "gigi's cupcakes", "gloria jean's coffee", "golden pride bbq chicken & ribs",
            "golden pride bbq chicken and ribs", "golden pride bbq chicken n ribs", "golf usa",
            "grand avenue market", "green mill restaurant", "griff's", "griffs", "grotto pizza",
            "grub burger bar", "gucci", "haagen-dazs", "havoline xpress lube", "head west", "hemline",
            "henig furs", "holsum bakery thrift store", "home cooking", "honey dew donuts",
            "honeydew donuts", "hot box pizza", "hotbox pizza", "hot mama", "human bean on rossanley",
            "husson's pizza", "iceburg drive-inn", "icing by claire", "industrial rideshop",
            "interstate brands", "it's fashion metro", "its fashion metro", "ivar's seafood bar",
            "ivars seafood bar", "j.p. licks", "jp licks", "j.p licks", "jp. licks", "jack's restaurant",
            "jacks restaurant", "jacks family restaurant", "james perse", "janie & jack", "janie and jack",
            "janie n jack", "januzzi's pizza", "jarman shoe co", "jiffy lube", "jiffylube", "jim simone",
            "john's incredible pizza", "johns incredible pizza", "johnny's pizza", "johnnys pizza",
            "johnston & murphy", "johnston and murphy", "johnston n murphy", "journeys", "journey's",
            "juice 4 u", "justice island walk", "justice just for girls", "k & g fashion superstore",
            "k n g fashion superstore", "k and g fashion superstore", "keebler co bakery",
            "keke's breakfast cafe", "kekes breakfast cafe", "ken's kickin chicken",
            "kens kickin chicken", "kicksusa", "kiehl's since 1851", "kiehls since 1851",
            "kilwins chocolates", "kings family restaurant", "krystal restaurants",
            "kum and go", "kum n go", "kum & go", "la plage swimwear", "lacoste", "ladida",
            "lamar's donuts & coffee", "lamar's donuts n coffee", "lamar's donuts and coffee",
            "lamars donuts & coffee", "lamars donuts n coffee", "lamars donuts and coffee",
            "larosa's pizzeria fairfield", "larosas pizzeria fairfield", "larry's meat & produce",
            "larrys meat & produce", "larry's meat and produce", "larrys meat n produce",
            "larry's meat n produce", "larrys meat and produce", "lazer fx", "legal sea foods",
            "lerner new york", "levi's store", "levis store", "lewis bakeries", "lf stores",
            "lil' caesar's", "lil caesars", "lil caesar's", "lil' caesars", "little caesars",
            "little caesar's", "limited store", "limted the", "the limted", "lou malnati's pizzeria",
            "lou malnatis pizzeria", "loving hut", "mad mex", "malcolm's haircutters",
            "malcolms haircutters", "malley's chocolate", "malleys chocolate", "mandee shop",
            "marc ecko", "marine layer", "marsh suprmkts", "marshall-rousso", "martin's super market",
            "martins super market", "maurice's gourmet barbeque", "maurices gourmet barbeque",
            "medford coffee company", "melrose", "melvin's bbq", "melvins bbq", "midas",
            "moe's southwest grill", "moes southwest grill", "mooyah burgers", "morristown running company",
            "movie time video", "moviestop", "mr empanada", "mr. burch formal wear",
            "mr burch formal wear", "mrs field's cookies",
            "mrs fields cookies", "mrs. field's cookies", "mrs. fields cookies", "murphy's deli",
            "murphys deli", "my kids closet", "my mamas sweet potato pie",
            "my mama's sweet potato pie", "nautica", "new hope apparel", "new york & co.",
            "new york and co", "new york n co", "nichols dollar saver", "nickles bakery thrift store",
            "nine west outlet", "oakley", "ocean beauty seafood", "omega protein", "one man band diner",
            "origins natural resources", "oshkosh b'gosh", "oshkosh bgosh", "ou-gah", "pac sun",
            "pacific sunwear", "pacsun", "pancake cabin", "panda chinese fast food", "pandora's box",
            "pandoras box", "papuchos snack", "paradiso", "pasta bravo", "paul assembly row",
            "pdq falls of neuse", "pendleton woolen mil", "perfect cuts", "perry ellis",
            "piatti restaurant", "piola", "planet hollywood", "plato's closet", "platos closet",
            "podnuh's bar-b-q", "podnuhs bar-b-q", "prada", "pretty girl", "primanti brothers",
            "pronto pizza", "publix super market", "pure elegance", "quik stop food mart", "ragstock",
            "ralph's famous italian ices", "ralphs famous italian ices", "rave", "repeat boutique",
            "rick's bbq", "ricks bbq", "rita's italian ice", "ritas italian ice", "roadrunner",
            "roanoke marshes trading", "robinsons mobil mart", "robinson's mobil mart", "rocket bakery",
            "rocket fizz", "rockport factory direct", "rockvale outlet", "macaroni grill",
            "royal caribbean bakery", "royal male", "runnings of pierre", "russell's of petoskey",
            "russells of petoskey", "rvca", "sacred natural healing", "saltwater house seafood & grille",
            "saltwater house seafood and grille", "saltwater house seafood n grille",
            "sam's wholesale club", "sams wholesale club", "samsonite", "sandal factory",
            "savers", "savory spice shop", "sbarro's italian eatery", "sbarros italian eatery",
            "scandinavian design", "schutz", "schuylkill valley sporting goods", "scooter's coffee",
            "scooters coffee", "scooters java", "scooter's java", "hallmark", "scrub pro uniforms",
            "seasons live action buffet", "second amendment sports", "seno", "servatii pastry shop",
            "shades sunglasses and casual apparel", "shades sunglasses n casual apparel",
            "shades sunglasses & casual apparel", "sharis", "shi by journey", "shiekh",
            "shoe department encore", "the shoe department", "shoe department, the",
            "shoe department the", "shoe depot", "shoe dept #", "shoe dept the", "shoe dept, the",
            "shoe kingdom, the", "shoe kingdom the", "shoe sensation", "shoe store, the",
            "shoe store the", "the shoe store", "shoe zone", "shoebilee", "shoefly shoe salons",
            "shoemax shoe store", "birkenstock", "shuhan music and clothing", "shuhan music n clothing",
            "shuhan music & clothing", "sid boedeker safety shoe service", "sienna sulla piazza",
            "simple elegance", "sirens", "six:02", "size 5-7-9 shops", "sketchers", "ski barn",
            "slot wholesale", "smart z'coil footwear", "smart zcoil footwear", "sophia eugene",
            "souplantation", "spanx store", "speciality minerals", "spencer gifts", "spencers",
            "spencer's", "spencers'", "spencers gift", "spencer's gift", "spencers' gift", "splatball",
            "squisito pizza and pasta", "squisito pizza n pasta", "squisito pizza & pasta",
            "stack'em high pancakes", "stackem high pancakes", "steel's fudge", "steels fudge",
            "stefan kaelin", "stellie bellies", "step ahead", "stevi b's pizza", "stevi bs pizza",
            "streetgame", "stride rite bootery", "stuart weitzman", "styles for less",
            "sun and ski sports", "sun n ski sports", "sun & ski sports", "sun glass plus",
            "suncoast pictures", "sunglass hut", "sunsations", "super shoe factory outlet",
            "super shoe stores", "super stop sporting goods", "super video", "surf city squeeze",
            "swank gift baskets", "sweet dreams candy co", "swim n sport", "swim & sport",
            "swim and sport", "synergy organic clothing", "taco johns", "takken's shoes",
            "takkens shoes", "talbots", "talbot's", "talbots the", "talbot's the", "talbots, the",
            "talbot's, the", "the talbot's", "the talbots", "taylor ann", "ted baker london",
            "teriyaki madness", "texas burger", "the athletes foot", "athletes foot",
            "the athletic fitters", "athletic fitters", "the avenue", "avenue, the", "avenue the",
            "the bean bag chair outlet", "bean bag chair outlet", "black dog, the", "black dog the",
            "the boys depot", "boys depot, the", "boys depot the", "the bread shop", "bread shop, the",
            "bread shop the", "the finish line", "finish line", "frye company", "grab bag co",
            "harbor hatter", "the icing", "icing, the", "icing the", "the indiana shop",
            "indiana shop the", "indiana shop, the", "the kids sale", "the lady bag", "the meadows",
            "the niche", "puma store", "rockport store", "shoe warehouse", "theshoe department",
            "things remembered", "thrifty shopper", "timberland", "toledo showroom", "tomas maier",
            "tops friendly market", "top's friendly market", "topshop", "torrid", "tortilla town",
            "track n trail", "track and trail", "track & trail", "trade home shoe", "traffic shoe",
            "trailhead", "treasure hunt", "trend boutique", "triple c leather", "tropical nut & fruit company",
            "tropical nut n fruit company", "tropical nut and fruit company", "tropical smoothie",
            "tropicana", "tse factory", "twice upon a time", "underground station", "united retail",
            "united states trading", "untouchable apparel", "up's & down", "up's n down", "up's and down",
            "ups & down", "ups n down", "ups and down", "us kids inc", "usa fitness direct",
            "valentino boutique", "van heusen", "vanity store", "vapor shark", "vf outlet",
            "video express", "video kingdom", "vmv hypoallergenics", "vollbracht furs", "walking co the",
            "warehouse market", "wemco factory store", "west side pizza and carry out",
            "west side pizza & carry out", "west side pizza n carry out", "western new york produce",
            "western warehouse", "westport big & tall", "westport big and tall", "westport big n tall",
            "when pigs fly", "whit's frozen custard", "whits frozen custard", "wights sporting goods",
            "wight's sporting goods", "wild willy's burger", "wild willys burger", "wilson sporting good",
            "wind river", "wolves within", "women's retail apparel", "womens retail apparel",
            "woof gang bakery n grooming", "woof gang bakery and grooming",
            "woof gang bakery & grooming", "world impact thrift store", "worth ny", "xxi forever",
            "yale comfort shoe", "yankee candle", "yankees clubhouse", "yee haw!", "yogo berry", "zumiez"]

        only_match_co_names = [
            "amf", "Aldo", "Avenue",
            "Buckle",
            "Catherine's", "Chico's",
            "Dots",
            "Express",
            "limited",
            "Vanity", "villa", "LOFT"]

        only_match = [
            "Antique Store", "Art Gallery",
            "Bridal Shop",
            "Book Store",
            "Chiropractor",
            "Department Store",
            "florist", "florist, gift shop", "Formal Wear",
            "Gift Shop, Home Decor", "Home Decor", "Home Improvement, Swimming Pool",
            "Library",
            "Medical & Health", "Night Club",
            "Pet Service, Medical & Health",
            "Shopping Mall", "Skin Care",
            "Trophies & Engraving"]

        match_any = [
            "accountant", "advertising agency", "advertising service", "aerospace/defense",
            "agricultural service", "airline", "airport", "animal shelter", "Aesthetics",
            "apartment & condo building", "appliance", "architect", "artistic services",
            "arts & marketing", "audiologist", "audiovisual equipment", "adult entertainment",
            "assembly of god",
            "bank", "bed and breakfast", "boat dealer", "bridal shop", "Boat Service",
            "broadcasting & media production", "brokers & franchising", "Boat Rental",
            "Beach Resort",
            "business consultant", "big box retailer", "Bed and Breakfast", "Baptist Church",
            "campground", "car dealership", "cargo & freight", "carpet", "Corporate Office",
            "carpet & flooring store", "casino", "charity organization", "City",
            "chemicals & gasses", "child care", "church", "city", "cleaning services",
            "clinic", "college", "commercial", "communitiy center", "construction",
            "consultant", "consulting/business services", "contractor", "concert venue",
            "copying & printing", "counseling", "Chiropractor", "cable & satellite service",
            "damage restoration", "day care", "dentist", "dermatologist", "doctor",
            "dog training", "dog walker", "drinking water distribution",
            "Dance Instruction", "department store",
            "education", "electrician", "elementary school", "employment agency",
            "engineering service", "Email Marketing", "Event Planning", "Event Venue",
            "family medicine practice", "financial services", "fire station", "Farm", "florist",
            "furniture store", "Food & Beverage Service & Distribution", "flea market",
            "fairground", "fitness center",
            "garden center", "government", "graphic design", "Glass Service", "gas & chemical service",
            "hardware & tools service", "health agency", "heating", "high school",
            "historical place", "home inspection", "horses", "hospital", "hostel", "hotel",
            "Home Window Service",
            "inn", "insurance agent", "insurance broker", "interior designer", "internet service provider",
            "jewelry store",
            "kennel", "kitchen supplies",
            "landscaping", "laser hair removal", "laundromat", "law practice", "loans", "lobbyist",
            "locksmith", "lodging", "Limo Service",
            "makeup artist", "management service", "manufacturing", "marketing consultant",
            "meeting room", "metals", "mission", "motel", "mover", "museum", "Medical & Health",
            "music lessons & instruction", "music production", "Marina", "martial arts",
            "media/news/publishing",
            "newspaper", "non-profit organization", "nursing", "Nutritionist", "neighborhood",
            "obgyn", "office supplies", "optometrist", "orchestra", "organization",
            "packaging supplies & equipment",
            "painter", "park", "pest control", "pest control", "pet breeder",
            "pet cemetery", "pet sitter", "petroleum services", "pharmacy", "photographer",
            "photographic services & equipment", "physical therapist", "plastic surgery",
            "playground", "plumber", "podiatrist", "post office", "preschool", "Performance Venue",
            "printing service", "professional services", "property management", "public services",
            "publisher", "Personal Trainer", "Pregnancy & Childbirth Service", "police station",
            "Promotional Item Service",
            "real estate", "refrigeration sales & services", "religious", "residence",
            "resort", "rubber service & supply", "rv dealership", "rv park", "Research Service",
            "Recycling & Waste Management", "Religious Organization", "Real Estate Service",
            "retirement & assisted living facility",
            "school", "screen printing & embroidery", "sewing & seamstress", "stadium",
            "signs & banner service", "solar energy service", "storage",
            "supply & distribution services", "Sports Venue & Stadium", "social service",
            "School",
            "taxi", "technical institute", "textiles", "tour guide",
            "tourist information", "tours & sightseeing", "towing service",
            "travel & transportation", "travel agency", "trophies & engravings", "Transportation Service",
            "trophies & engravings", "Tire Dealer",
            "university", "Upholstery",
            "vacation home rental", "ventilating",
            "veterinarian", "Vending Machine Service",
            "warehouse", "water filtration", "web development", "wedding planning",
            "wholesale", "wholesale & supply store", "workplace & office", "Window Service & Repair",
            "Waste Management",
            "youth organization"]

        yelp_match_any = [
            "accountants", "adult", "advertising", "Anesthesiologists", "Animal Shelters",
            "animalshelters", "apartments", "Appraisal Services", "appraisalservices", "appraiser",
            "architects",
            "bankruptcylaw", "banks&creditunions", "Boat Dealers", "Boat Repair",
            "boatdealers", "boatrepair", "Business Consulting", "businesslaw", "cabaret",
            "cabinetry", "cannabisclinics", "Car Dealers", "Car Rental", "cardealers",
            "cardiologists", "careercounseling", "Carpet Cleaning", "Carpet Installation",
            "carpet_cleaning", "carpetcleaning", "Carpeting", "carrental", "Child Care & Day Care",
            "childcare&daycare", "Churches", "Colleges & Universities", "colleges&universities",
            "contractors", "Cosmetic Dentists", "Cosmetic Surgeons", "cosmeticdentists",
            "cosmeticsurgeons", "cosmetologyschools", "Counseling & Mental Health",
            "community center",
            "counseling&mentalhealth", "Couriers & Delivery Services", "couriers&deliveryservices",
            "criminaldefenselaw", "Cultural Center", "culturalcenter", "Damage Restoration",
            "damagerestoration", "dentists", "dermatologists", "Diagnostic Imaging", "diagnosticimaging",
            "diagnosticservices", "divorce&familylaw", "Doctors", "doorsales/installation", "duilaw",
            "Education", "Educational Services", "educationalservices", "electricians",
            "Elementary Schools", "elementaryschools", "emergencyrooms", "employmentagencies",
            "Endocrinologists", "estateplanninglaw", "Family Practice", "financialadvising",
            "financialservices", "fireplaceservices", "flooring", "Formal Wear", "formalwear",
            "financial planning",
            "Funeral Services & Cemeteries", "funeralservices&cemeteries", "General Dentistry",
            "generaldentistry", "generallitigation", "Graphic Design", "graphicdesign", "Head Shops",
            "Health Retreats", "Heating & Air Conditioning/HVAC", "heating&airconditioning/hvac",
            "Home Cleaning", "Home Theatre Installation", "homecleaning", "homehealthcare",
            "homeorganization", "homeservices", "hometheatreinstallation", "homewindowtinting",
            "Horse Boarding", "Horseback Riding", "horsebackriding", "horseboarding", "Hospitals",
            "hostels", "hotels", "Hotels & Travel", "hotels&travel", "hottub&pool", "immigrationlaw",
            "insurance", "Internet Service Providers", "internetserviceproviders", "investing",
            "irrigation", "Junk Removal & Hauling", "Junk-Dealers", "junkremoval&hauling",
            "Laboratory Testing", "Lactation Services", "lactationservices", "lawyers", "libraries",
            "Life Coach", "marketing", "masonry/concrete", "Mass Media", "Medical Centers",
            "Medical Clinics", "medicalcenters", "medicaltransportation", "Metal Fabricators",
            "metalfabricators", "middleschools&highschools", "Midwives", "mortgagebrokers", "movers",
            "Music Production Services", "Musical Instrument Services", "Musical Instruments & Teachers",
            "musicalinstruments&teachers", "musicians", "Notaries", "Observatories",
            "Obstetricians & Gynecologists", "obstetricians&gynecologists", "Occupational Therapy",
            "Officiants", "ophthalmologists", "Optometrists", "Oral Surgeons", "oralsurgeons",
            "orthodontists", "orthopedists", "orthotics", "osteopathicphysicians", "parentingclasses",
            "Pediatric Dentists", "Pediatricians", "Personal Assistants", "personalassistants",
            "personalinjurylaw", "Physical Therapy", "physicaltherapy", "podiatrists",
            "pool&hottubservice", "poolcleaners", "powdercoating", "preschools", "Pressure Washers",
            "Print Media", "Printing Services", "printingservices", "printmedia", "Private Tutors",
            "privateinvestigation", "privatetutors", "Professional Services", "professionalservices",
            "propane", "propertymanagement", "Psychics & Astrologers", "psychics&astrologers",
            "Public Relations", "publicrelations", "publicservices&government", "publictransportation",
            "Radio Stations", "radiostations", "realestateagents", "realestatelaw", "realestateservices",
            "realestatesvcs", "Recording & Rehearsal Studios", "recording&rehearsalstudios",
            "Recycling Center", "recyclingcenter", "Religious Organizations", "Religious Schools",
            "religiousorganizations", "religiousschools", "Restaurant Management", "Retirement Homes",
            "retirementhomes", "Rolfing", "roofing", "RV Dealers", "rvdealers", "rvparks", "rvrepair",
            "School & Text", "Screen Printing", "Screen Printing/T-Shirt Printing", "screenprinting",
            "screenprinting/t-shirtprinting", "Security Systems", "securitysystems", "sessionphotography",
            "shades&blinds", "shippingcenters", "shreddingservices", "signmaking", "skiresorts",
            "smogcheckstations", "snowremoval", "softwaredevelopment", "solarinstallation",
            "Special Education", "specialeducation", "Specialty Schools", "specialtyschools",
            "speechtherapists", "sportsmedicine", "Stadiums & Arenas", "stadiums&arenas",
            "Swimming Lessons/Schools", "televisionserviceproviders", "testpreparation", "towing",
            "treeservices", "tutoringcenters", "universityhousing", "urgentcare", "vacationrentals",
            "Video/Film Production", "video/filmproduction", "videographers", "waterdelivery",
            "webdesign", "Wedding Planning", "wedding_planning", "weddingplanning",
            "windowsinstallation", "yelpevents", "lasereyesurgery/lasik", "Laser Hair Removal",
            "laserhairremoval"]

        yelp_match_exactly = [
            "airports", "Animal Specialty Services", "appliances", "Appliances & Repair",
            "appliances&repair", "Art Galleries", "artgalleries", "Auction Houses", "auctionhouses",
            "boatcharters", "Bookstores", "Boot Camps", "bootcamps", "Bridal , Men's Clothing",
            "Bridal, Formal Wear", "Bridal, Women's Clothing", "bridal,flowers&gifts,weddingplanning",
            "bridal,formalwear", "bridal,formalwear", "bridal,jewelry", "bridal,men'sclothing",
            "bridal,menscloth", "bridal,women'sclothing", "campgrounds", "casinos", "Bridal Shops",
            "Check Cashing/Pay-day Loans", "checkcashing/pay-dayloans", "chiropractors",
            "Community Service/Non-Profit", "communityservice/non-profit", "dogparks", "dogwalkers",
            "editorialservices", "eventphotography", "galleries", "Interior Design", "interiordesign",
            "jewelry,bridal", "lakes", "landscaping", "Men's Clothing, Bridal", "menscloth,bridal",
            "Jewelry, jewelry, Art Galleries, galleries",
            "museums", "parks", "Performing Arts", "performingarts", "photographers", "planetarium",
            "playgrounds", "racetracks", "resorts", "Skateboards & Equipment", "Street Vendors",
            "streetvendors", "summercamps", "Tours", "Venues & Event Spaces", "venues&eventspaces",
            "veterinarians", "Weight Loss Centers", "weightlosscenters"]

        if fb_data:
            if fb_data.get('fuzzy_score') == "Wrong":
                data['Lead_Invalid_Reason__c'] = 'wrong fb page match'
                return (0, data['Lead_Invalid_Reason__c'][:50])

            # cat_list = self.get_category_list(fb_data.get('page_data'))
            for cat in match_any:
                if cat.lower() in cat_list.lower():
                    data['Lead_Invalid_Reason__c'] = "bad fb cat - " + cat
                    return (0, data['Lead_Invalid_Reason__c'][:50])

            for cat in only_match:
                if cat_list.lower() == cat.lower():
                    data['Lead_Invalid_Reason__c'] = "bad fb cat - " + cat
                    return (0, data['Lead_Invalid_Reason__c'][:50])

            if fb_data.get('is_permanently_closed'):
                data['Lead_Invalid_Reason__c'] = 'facebook biz closed'
                return (0, data['Lead_Invalid_Reason__c'][:50])

        for name_filter in neg_co_names:
            if name_filter.lower() in data['Company'].lower():
                data['Lead_Invalid_Reason__c'] = "bad co name - " + name_filter
                return (0, data['Lead_Invalid_Reason__c'][:50])

        for name_filter in only_match_co_names:
            if name_filter.lower() == data['Company'].lower():
                data['Lead_Invalid_Reason__c'] = "bad co name - " + name_filter
                return (0, data['Lead_Invalid_Reason__c'][:50])

        if data.get('Yelp_Category__c'):
            yelp_cat_list = str(data.get('Yelp_Category__c')).lower().replace(', ', ',').replace(' ', '').split(',')

            yelp_match_any = [cat.lower() for cat in yelp_match_any]
            for yelp_cat in yelp_match_any:
                if yelp_cat.lower() in yelp_cat_list:
                    data['Lead_Invalid_Reason__c'] = "bad yelp cat - " + yelp_cat
                    return (0, data['Lead_Invalid_Reason__c'][:50])

            yelp_match_exactly = [cat.lower().replace(', ', '') for cat in yelp_match_exactly]
            for yelp_cat in yelp_match_exactly:
                if yelp_cat.lower() == yelp_cat_list[0].lower().replace(', ', ''):
                    data['Lead_Invalid_Reason__c'] = "bad yelp cat - " + yelp_cat
                    return (0, data['Lead_Invalid_Reason__c'][:50])

        return (1, "Valid")
