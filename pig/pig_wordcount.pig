-- Step 1: Load input from HDFS
-- lines = LOAD '/user/thanos/pig_input/data.txt' USING PigStorage(' ') AS (word:chararray);

-- Step 2: Tokenize words
-- words = FOREACH lines GENERATE FLATTEN(TOKENIZE(word)) AS word;

-- Step 3: Group by each word
-- groups = GROUP words BY word;

-- Step 4: Count occurrences
-- wordcount = FOREACH groups GENERATE group, COUNT(words);

-- Step 5: Store output in HDFS
-- STORE wordcount INTO '/user/thanos/pig_output';


-- ===============================================================
-- PIG WORDCOUNT SCRIPT (Final verified for Aditya–Parth cluster)
-- Handles punctuation, numbers, mixed case, and extra spaces
-- Hadoop 3.3.6 + Pig 0.18.0
-- ===============================================================

-- Step 1️⃣: Load the input text file from HDFS
lines = LOAD '/user/thanos/pig_input/data.txt' USING PigStorage('\n') AS (line:chararray);

-- Step 2️⃣: Convert all text to lowercase to ensure uniform counting
lowered = FOREACH lines GENERATE LOWER(line) AS line;

-- Step 3️⃣: Split text using REGEX on any character that is NOT a letter or digit
-- This will separate words cleanly and also remove punctuation.
-- To ignore numbers, change the regex to '[^a-zA-Z]+' instead of '[^a-zA-Z0-9]+'
--tokens = FOREACH lowered GENERATE FLATTEN(STRSPLIT(line, '[^a-zA-Z0-9]+')) AS word;
tokens = FOREACH lowered GENERATE FLATTEN(STRSPLIT(line, '[^a-zA-Z]+')) AS word;

-- Step 4️⃣: Remove any empty or null tokens
cleaned = FILTER tokens BY word IS NOT NULL AND word != '';

-- Step 5️⃣: Group all identical words together
grouped = GROUP cleaned BY word;

-- Step 6️⃣: Count number of occurrences of each word
wordcount = FOREACH grouped GENERATE group AS word, COUNT(cleaned) AS count;

-- Step 7️⃣: Store the result in HDFS (overwrites if path already exists)
STORE wordcount INTO '/user/thanos/pig_output' USING PigStorage('\t');
