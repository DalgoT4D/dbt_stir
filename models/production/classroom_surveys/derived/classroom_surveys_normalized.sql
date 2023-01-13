{{ config(
  materialized='table'

) }}


with merged_normalized AS (SELECT "KEY","submissiondate","country","region","sub_region","forms","observation_term","meeting",
       unnest(array['s1','s2','s3','s4','c1','c2','e1','e2','se1','se2','se3','se4','se5','cc1','cc2', 'cc3', 'cc4', 'cc5','ad1', 'ad2', 'ad3', 'ad4', 'ad5', 'ad6','ad7', 'ad8', 'ad9','sr1', 'sr2', 'sr3', 'sr4', 'sr5', 'sr6']) AS subindicator,
       unnest(array[s1,s2,s3,s4,c1,c2,e1,e2,se1,se2,se3,se4,se5,cc1,cc2,cc3, cc4,cc5,ad1, ad2, ad3, ad4, ad5, ad6,ad7, ad8, ad9,sr1, sr2, sr3, sr4, sr5, sr6]) AS score
FROM {{ref('classroom_surveys_merged')}}
)

select *, 
CASE 
WHEN subindicator IN ('s1','s2','s3','s4') THEN 'Safety' 
WHEN subindicator IN ('c1','c2') THEN 'Curiosity & Critical Thinking'
WHEN subindicator IN ('e1','e2') THEN 'Engagement'
WHEN subindicator IN ('se1','se2', 'se3', 'se4', 'se5') THEN 'Self Esteem'
WHEN subindicator IN ('cc1','cc2', 'cc3', 'cc4', 'cc5') THEN 'Intentional Teaching'
WHEN subindicator IN ('ad1', 'ad2', 'ad3', 'ad4', 'ad5', 'ad6','ad7', 'ad8', 'ad9') THEN 'Delhi Additional Indicators'
WHEN subindicator IN ('sr1', 'sr2', 'sr3', 'sr4', 'sr5', 'sr6') THEN 'Delhi Co-ART Meetings'
ELSE 'Other' END AS behavior from merged_normalized
ORDER BY "submissiondate","KEY"
