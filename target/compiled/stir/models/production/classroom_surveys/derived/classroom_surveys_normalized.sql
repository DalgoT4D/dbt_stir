


with merged_normalized AS (SELECT "KEY","submissiondate","country","region","sub_region","forms","observation_term","meeting",
       unnest(array['s1','s2','s3','s4','c1','c2','e1','e2','se1','se2','se3','se4','se5']) AS subindicator,
       unnest(array[s1,s2,s3,s4,c1,c2,e1,e2,se1,se2,se3,se4,se5]) AS score
FROM "postgres"."prod"."classroom_surveys_merged"
)

select *, 
CASE 
WHEN subindicator IN ('s1','s2','s3','s4') THEN 'Safety' 
WHEN subindicator IN ('c1','c2') THEN 'Curiosity & Critical Thinking'
WHEN subindicator IN ('e1','e2') THEN 'Engagement'
WHEN subindicator IN ('se1','se2', 'se3', 'se4', 'se5') THEN 'Engagement'
ELSE 'Other' END AS behavior from merged_normalized
ORDER BY "submissiondate","KEY"