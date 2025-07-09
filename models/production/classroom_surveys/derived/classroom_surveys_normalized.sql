{{ config(
    materialized = 'table',
    schema       = generate_schema_name('prod', this)
) }}

------------------------------------------------------------------------------
-- 1️⃣  Raw‑to‑long transformation
------------------------------------------------------------------------------
WITH merged_normalized AS (

    SELECT
        "KEY",
        "malepresent",
        "femalepresent",
        "submissiondate",
        "observation_date",
        "remarks_qualitative",
        "country",
        "region",
        "sub_region",
        "program",
        "forms",
        "forms_verbose",
        "forms_verbose_consolidated",
        "observation_term",
        "plname",
        "education_level",
        "meeting",
        "role_coaching",

        /* ----------------- unpivot the subindicator / score columns ----------------- */
        UNNEST ( ARRAY[
            's1','s2','s3','s4',
            'c1','c2','c3',
            'e1','e2','e3',
            'se1','se2','se3','se4','se5',
            'cc1','cc2','cc3','cc4','cc5',
            'gc1','gc2','gc3','gc4','gc5',
            'ad1','ad2','ad3','ad4','ad5','ad7','ad8','ad9',
            'sr1','sr2','sr3','sr4','sr5','sr6',

            -- CRO‑13 groups
            'cro13ai_behavior_engagement','cro13ai_behavior_safety','cro13ai_behavior_selfesteem',
            'cro13ai_building_a_stronger_community','cro13ai_building_connect','cro13ai_classroom_routines',
            'cro13ai_lesson_planning','cro13ai_look_for_understanding___respond','cro13ai_mission_buniyad',
            'cro13ai_psychological_safety','cro13ai_social___emotional_wellbeing',
            'cro13ai_teaching___learning_strategies_1','cro13ai_teaching___learning_strategies_2',

            'cro13aiii_asking_effective_questions','cro13aiii_elaborative_questioning','cro13aiii_emotional_learning_environment',
            'cro13aiii_growth_mindset','cro13aiii_physical_learning_environment','cro13aiii_retrieval_practices',
            'cro13aiii_worked_examples',

            'cro13aiv_bridging_covid19_learning_losses','cro13aiv_classroom_routines','cro13aiv_longterm_learning',
            'cro13aiv_na','cro13aiv_socio_emotional_wellbeing','cro13aiv_structuring_learning_journey',

            'cro13av_growth_mindset','cro13av_normalising_error',
            'cro13b','cro13c'
        ] )                                         AS subindicator,

        UNNEST ( ARRAY[
            s1,s2,s3,s4,
            c1,c2,c3,
            e1,e2,e3,
            se1,se2,se3,se4,se5,
            cc1,cc2,cc3,cc4,cc5,
            gc1,gc2,gc3,gc4,gc5,
            ad1,ad2,ad3,ad4,ad5,ad7,ad8,ad9,
            sr1,sr2,sr3,sr4,sr5,sr6,

            cro13ai_behavior_engagement,cro13ai_behavior_safety,cro13ai_behavior_selfesteem,
            cro13ai_building_a_stronger_community,cro13ai_building_connect,cro13ai_classroom_routines,
            cro13ai_lesson_planning,cro13ai_look_for_understanding___respond,cro13ai_mission_buniyad,
            cro13ai_psychological_safety,cro13ai_social___emotional_wellbeing,
            cro13ai_teaching___learning_strategies_1,cro13ai_teaching___learning_strategies_2,

            cro13aiii_asking_effective_questions,cro13aiii_elaborative_questioning,cro13aiii_emotional_learning_environment,
            cro13aiii_growth_mindset,cro13aiii_physical_learning_environment,cro13aiii_retrieval_practices,
            cro13aiii_worked_examples,

            cro13aiv_bridging_covid19_learning_losses,cro13aiv_classroom_routines,cro13aiv_longterm_learning,
            cro13aiv_na,cro13aiv_socio_emotional_wellbeing,cro13aiv_structuring_learning_journey,

            cro13av_growth_mindset,cro13av_normalising_error,
            cro13b,cro13c
        ] )                                         AS score

    FROM {{ ref('classroom_surveys_merged') }}

),

------------------------------------------------------------------------------
-- 2️⃣  Deterministic surrogate key: "KEY" + subindicator  → key_subindicator_key
------------------------------------------------------------------------------
with_primary_key AS (

    SELECT
        *,
        {{ dbt_utils.generate_surrogate_key(['"KEY"', 'subindicator']) }} AS key_subindicator_key
        -- ⇡ If dbt_utils is not installed, fallback:
        -- CONCAT("KEY", '_', subindicator)  AS key_subindicator_key
    FROM merged_normalized
),

------------------------------------------------------------------------------
-- 3️⃣  Add behavior bucket & ignore NULL scores
------------------------------------------------------------------------------
classified AS (

    SELECT
        wp.*,

        CASE
            WHEN subindicator IN ('s1','s2','s3','s4')
                 THEN 'Safety'
            
            WHEN subindicator IN ('c1')
                 THEN 'Curiosity & Critical Thinking and Engagement'

            WHEN subindicator IN ('c2','c3')
                 THEN 'Curiosity & Critical Thinking'

            WHEN subindicator IN ('e1','e2')
                 THEN 'Engagement'

            WHEN subindicator IN ('se1','se2','se3','se4','se5')
                 THEN 'Self Esteem'

            WHEN subindicator IN ('cc1','cc2','cc3','cc4','cc5',
                                  'gc1','gc2','gc3','gc4','gc5')
                 THEN 'Intentional Teaching'

            WHEN subindicator IN ('ad1','ad2','ad3','ad4','ad5','ad7','ad8','ad9')
                 THEN 'Delhi Additional Indicators'

            WHEN subindicator IN ('sr1','sr2','sr3','sr4','sr5','sr6')
                 THEN 'Delhi Co‑ART Meeting Indicators'

            WHEN subindicator IN (
                 'cro13ai_behavior_engagement','cro13ai_behavior_safety','cro13ai_behavior_selfesteem',
                 'cro13ai_building_a_stronger_community','cro13ai_building_connect','cro13ai_classroom_routines',
                 'cro13ai_lesson_planning','cro13ai_look_for_understanding___respond','cro13ai_mission_buniyad',
                 'cro13ai_psychological_safety','cro13ai_social___emotional_wellbeing',
                 'cro13ai_teaching___learning_strategies_1','cro13ai_teaching___learning_strategies_2',

                 'cro13aiii_asking_effective_questions','cro13aiii_elaborative_questioning',
                 'cro13aiii_emotional_learning_environment','cro13aiii_growth_mindset',
                 'cro13aiii_physical_learning_environment','cro13aiii_retrieval_practices',
                 'cro13aiii_worked_examples',

                 'cro13aiv_bridging_covid19_learning_losses','cro13aiv_classroom_routines',
                 'cro13aiv_longterm_learning','cro13aiv_na','cro13aiv_socio_emotional_wellbeing',
                 'cro13aiv_structuring_learning_journey',

                 'cro13av_growth_mindset','cro13av_normalising_error',
                 'cro13b','cro13c'
            )
                 THEN 'Additional CRO Indicators'

            ELSE 'Other'
        END                                                   AS behavior

    FROM with_primary_key  AS wp
    WHERE score IS NOT NULL
),

------------------------------------------------------------------------------
-- 3️⃣.1  Transform scores for specific subindicators and forms
------------------------------------------------------------------------------
score_transformed AS (
    SELECT 
        classified.*,
        CASE 
            WHEN forms IN ('cro_ug', 'cro', 'cro_indo') 
            AND subindicator IN ('s1', 's2')
            AND score = 3 THEN 1
            WHEN forms IN ('cro_ug', 'cro', 'cro_indo') 
            AND subindicator IN ('s1', 's2')
            AND score = 1 THEN 3
            ELSE score
        END as transformed_score
    FROM classified
),

------------------------------------------------------------------------------
-- 4️⃣  Deduplicate: keep the *latest* row per key_subindicator_key
------------------------------------------------------------------------------
deduped AS (

    SELECT 
        key_subindicator_key,
        "KEY",
        malepresent,
        femalepresent,
        submissiondate,
        observation_date,
        remarks_qualitative,
        country,
        region,
        sub_region,
        program,
        forms,
        forms_verbose,
        forms_verbose_consolidated,
        observation_term,
        plname,
        education_level,
        meeting,
        role_coaching,
        subindicator,
        transformed_score as score,
        behavior
    FROM (
        SELECT
            score_transformed.*,
            ROW_NUMBER() OVER (
                PARTITION BY key_subindicator_key
                ORDER BY submissiondate DESC        -- earliest → ASC
            ) AS rn
        FROM score_transformed
    ) ranked
    WHERE rn = 1
)

------------------------------------------------------------------------------
-- 5️⃣  Final result
------------------------------------------------------------------------------
SELECT *
FROM   deduped
ORDER  BY submissiondate, "KEY"
