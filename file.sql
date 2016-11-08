SELECT COUNT(*) FROM publicdata:samples.wikipedia; 
SELECT COUNT(*) FROM publicdata:samples.wikipedia;
SELECT COUNT(*) FROM publicdata:samples.wikipedia;
SELECT word, word_count, corpus 
FROM publicdata:samples.shakespeare 
WHERE (REGEXP_MATCH(word,r'[A-Z]')) 
AND LENGTH(word) > 3 
ORDER BY word_count DESC LIMIT 2000;
