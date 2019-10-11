class SqlQueries:
    
    create_staging_listings = ("""
        DROP TABLE IF EXISTS STG_LISTINGS;
        CREATE TABLE STG_LISTINGS (
                                LISTING_ID INT NOT NULL,
                                HOST_ID INT,
                                HOST_URL VARCHAR,
                                HOST_NAME VARCHAR,
                                HOST_SINCE DATE,
                                HOST_RESPONSE_TIME VARCHAR,
                                HOST_IS_SUPERHOST VARCHAR,
                                HOST_TOTAL_LISTINGS_COUNT FLOAT,  
                                NEIGHBOURHOOD VARCHAR,
                                PROPERTY_TYPE VARCHAR,
                                ROOM_TYPE VARCHAR,
                                BEDS FLOAT,
                                GUESTS_INCLUDED INT,
                                NUMBER_OF_REVIEWS INT,
                                REVIEW_SCORES_RATING FLOAT,       
                                CANCELLATION_POLICY VARCHAR,
                                REVIEWS_PER_MONTH FLOAT,
                                HOST_IDENTITY_VERIFIED VARCHAR)
    """)
    create_staging_calendars = ("""
        DROP TABLE IF EXISTS STG_CALENDARS;
        CREATE TABLE STG_CALENDARS (
                                LISTING_ID INT NOT NULL,
                                CALENDAR_DATE DATE,
                                AVAILABLE VARCHAR,
                                PRICE INT,
                                ADJUSTED_PRICE INT,
                                MINIMUM_NIGHTS INT,
                                MAXIMUM_NIGHTS INT)
    """)
    create_staging_reviews = ("""
       DROP TABLE IF EXISTS STG_REVIEWS;
       CREATE TABLE STG_REVIEWS (
                                LISTING_ID INT NOT NULL,
                                REVIEW_ID INT,
                                REVIEW_DATE DATE,
                                REVIEWER_ID INT,
                                REVIEWER_NAME VARCHAR,
                                COMMENT varchar(max))
    """)
    create_staging_neighbourhoods = ("""
       DROP TABLE IF EXISTS REF_NEIGHBOURHOODS;
       CREATE TABLE REF_NEIGHBOURHOODS (
                                NEIGHBOURHOOD_ID INT,
                                NEIGHBOURHOOD_NAME VARCHAR)
    """)
    
    CREATE_DIM_HOSTS = ("""
       CREATE TABLE public.DIM_HOSTS (
       HOST_ID VARCHAR NOT NULL,
       HOST_NAME VARCHAR,
       HOST_URL VARCHAR,
       HOST_SINCE DATE,
       HOST_RESPONSE_TIME VARCHAR,
       HOST_IS_SUPERHOST VARCHAR,
       HOST_TOTAL_LISTINGS_COUNT INT,
       LISTING_ID INT,
       CONSTRAINT DIM_HOSTS_PKEY PRIMARY KEY (HOST_ID) );
    """)

    CREATE_DIM_REVIEWS = ("""
       CREATE TABLE public.DIM_REVIEWS (
       REVIEW_ID INT NOT NULL,
       LISTING_ID INT,
       REVIEWER_ID INT,
       REVIEW_DATE DATE,
       COMMENT VARCHAR(MAX),
       REVIEW_SCORES_RATING FLOAT,
       CONSTRAINT DIM_REVIEWS_PKEY PRIMARY KEY (REVIEW_ID) );
    """)

    CREATE_DIM_CALENDARS = ("""
       CREATE TABLE public.DIM_CALENDARS (
       CALENDAR_ID INT IDENTITY(1,1) NOT NULL,
       CALENDAR_DATE DATE,
       AVAILABLE VARCHAR,
       ADJUSTED_PRICE INT,
       MINIMUM_NIGHTS INT,
       MAXIMUM_NIGHTS INT,
       LISTING_ID INT,
       CONSTRAINT DIM_CALENDARS_PKEY PRIMARY KEY (CALENDAR_ID) );
    """)

    CREATE_DIM_PROPERTIES = ("""
       CREATE TABLE public.DIM_PROPERTIES (
       LISTING_ID INT NOT NULL,
       ROOM_TYPE VARCHAR,
       PROPERTY_TYPE VARCHAR,
       GUSTS_INCLUDED INT,
       CANCELLATION_POLICY VARCHAR,
       HOST_ID INT,
       NEIGHBOURHOOD_ID INT,
       CONSTRAINT DIM_PROPERTIES_PKEY PRIMARY KEY (LISTING_ID) );
    """)

    CREATE_LOAD_FACT_AIRBNB_AMST = ("""
       CREATE TABLE FACT_AIRBNB_AMST (
       FACT_ID INT IDENTITY(1,1) NOT NULL,
       HOST_ID INT,
       LISTING_ID INT,
       NUMBER_OF_REVIEWS INT,
       AVG_REVIEWS_RATING FLOAT,
       CONSTRAINT FACT_AIRBNB_AMST_PKEY PRIMARY KEY (FACT_ID) );
       INSERT INTO FACT_AIRBNB_AMST
       (HOST_ID, LISTING_ID, NUMBER_OF_REVIEWS, AVG_REVIEWS_RATING)
       SELECT p.HOST_ID, p.LISTING_ID, COUNT(r.REVIEW_ID), AVG(r.REVIEW_SCORES_RATING)
       FROM DIM_PROPERTIES P
       INNER JOIN REF_NEIGHBOURHOODS REF
       ON REF.NEIGHBOURHOOD_ID = P.NEIGHBOURHOOD_ID
       INNER JOIN DIM_REVIEWS R
       ON R.listing_id = P.listing_id
       GROUP BY P.HOST_ID, P.LISTING_ID;
    """)
    
    properties_table_insert = ("""
       INSERT INTO DIM_PROPERTIES
       SELECT DISTINCT l.LISTING_ID, l.ROOM_TYPE, l.PROPERTY_TYPE, l.GUESTS_INCLUDED, l.CANCELLATION_POLICY, l.HOST_ID, ref.NEIGHBOURHOOD_ID
       FROM stg_listings l
       INNER JOIN ref_neighbourhoods ref
       ON l.neighbourhood = ref.neighbourhood_name
       WHERE l.host_identity_verified = 't'
    """)
    calendars_table_insert = ("""
       INSERT INTO DIM_CALENDARS 
       (CALENDAR_DATE, AVAILABLE, ADJUSTED_PRICE, MINIMUM_NIGHTS, MAXIMUM_NIGHTS, LISTING_ID)
       SELECT DISTINCT CALENDAR_DATE, AVAILABLE, ADJUSTED_PRICE, MINIMUM_NIGHTS, MAXIMUM_NIGHTS, LISTING_ID
       FROM STG_CALENDARS
    """)
    reviews_table_insert = ("""
       INSERT INTO DIM_REVIEWS
       SELECT DISTINCT r.REVIEW_ID, r.LISTING_ID, r.REVIEWER_ID, r.REVIEW_DATE, r.COMMENT, l.REVIEW_SCORES_RATING
       FROM stg_reviews r
       INNER JOIN stg_listings l
       ON l.listing_id = r.listing_id
       where r.comment is not null
    """)
    hosts_table_insert = ("""
       INSERT INTO DIM_HOSTS
       SELECT DISTINCT HOST_ID, HOST_NAME, HOST_URL, HOST_SINCE, HOST_RESPONSE_TIME, HOST_IS_SUPERHOST, HOST_TOTAL_LISTINGS_COUNT, LISTING_ID
       FROM stg_listings
       WHERE HOST_NAME <> 'Not Provided'
       AND HOST_SINCE <> '1990-01-01'
    """)