{{ config(
    materialized = 'table'
) }}

------------------------------------------------------------------------------
-- Raw‑to‑long transformation
------------------------------------------------------------------------------
WITH expanded_cro13 AS (
    SELECT *,
        -- Expand cro13ai space-separated values into individual binary indicators
        CASE WHEN cro13ai LIKE '%behavior_engagement%' THEN 1 ELSE 0 END as cro13ai_behavior_engagement,
        CASE WHEN cro13ai LIKE '%behavior_safety%' THEN 1 ELSE 0 END as cro13ai_behavior_safety,
        CASE WHEN cro13ai LIKE '%behavior_selfesteem%' THEN 1 ELSE 0 END as cro13ai_behavior_selfesteem,
        CASE WHEN cro13ai LIKE '%building_a_stronger_community%' THEN 1 ELSE 0 END as cro13ai_building_a_stronger_community,
        CASE WHEN cro13ai LIKE '%building_connect%' THEN 1 ELSE 0 END as cro13ai_building_connect,
        CASE WHEN cro13ai LIKE '%classroom_routines%' THEN 1 ELSE 0 END as cro13ai_classroom_routines,
        CASE WHEN cro13ai LIKE '%lesson_planning%' THEN 1 ELSE 0 END as cro13ai_lesson_planning,
        CASE WHEN cro13ai LIKE '%look_for_understanding%' THEN 1 ELSE 0 END as cro13ai_look_for_understanding___respond,
        CASE WHEN cro13ai LIKE '%mission_buniyad%' THEN 1 ELSE 0 END as cro13ai_mission_buniyad,
        CASE WHEN cro13ai LIKE '%psychological_safety%' THEN 1 ELSE 0 END as cro13ai_psychological_safety,
        CASE WHEN cro13ai LIKE '%social_%emotional_wellbeing%' THEN 1 ELSE 0 END as cro13ai_social___emotional_wellbeing,
        CASE WHEN cro13ai LIKE '%teaching___learning_strategies_1%' THEN 1 ELSE 0 END as cro13ai_teaching___learning_strategies_1,
        CASE WHEN cro13ai LIKE '%teaching___learning_strategies_2%' THEN 1 ELSE 0 END as cro13ai_teaching___learning_strategies_2,

        -- Expand cro13aiii space-separated values
        CASE WHEN cro13aiii LIKE '%asking_effective_questions%' THEN 1 ELSE 0 END as cro13aiii_asking_effective_questions,
        CASE WHEN cro13aiii LIKE '%elaborative_questioning%' THEN 1 ELSE 0 END as cro13aiii_elaborative_questioning,
        CASE WHEN cro13aiii LIKE '%emotional_learning_environment%' THEN 1 ELSE 0 END as cro13aiii_emotional_learning_environment,
        CASE WHEN cro13aiii LIKE '%growth_mindset%' THEN 1 ELSE 0 END as cro13aiii_growth_mindset,
        CASE WHEN cro13aiii LIKE '%physical_learning_environment%' THEN 1 ELSE 0 END as cro13aiii_physical_learning_environment,
        CASE WHEN cro13aiii LIKE '%retrieval_practices%' THEN 1 ELSE 0 END as cro13aiii_retrieval_practices,
        CASE WHEN cro13aiii LIKE '%worked_examples%' THEN 1 ELSE 0 END as cro13aiii_worked_examples,

        -- Expand cro13aiv space-separated values
        CASE WHEN cro13aiv LIKE '%bridging_covid19_learning_losses%' THEN 1 ELSE 0 END as cro13aiv_bridging_covid19_learning_losses,
        CASE WHEN cro13aiv LIKE '%classroom_routines%' THEN 1 ELSE 0 END as cro13aiv_classroom_routines,
        CASE WHEN cro13aiv LIKE '%longterm_learning%' THEN 1 ELSE 0 END as cro13aiv_longterm_learning,
        CASE WHEN cro13aiv LIKE '%na%' THEN 1 ELSE 0 END as cro13aiv_na,
        CASE WHEN cro13aiv LIKE '%socio_emotional_wellbeing%' THEN 1 ELSE 0 END as cro13aiv_socio_emotional_wellbeing,
        CASE WHEN cro13aiv LIKE '%structuring_learning_journey%' THEN 1 ELSE 0 END as cro13aiv_structuring_learning_journey,

        -- Expand cro13av space-separated values
        CASE WHEN cro13av LIKE '%growth_mindset%' THEN 1 ELSE 0 END as cro13av_growth_mindset,
        CASE WHEN cro13av LIKE '%normalising_error%' THEN 1 ELSE 0 END as cro13av_normalising_error

    FROM {{ ref('classroom_surveys_merged') }}
),

merged_normalized AS (

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
            s1::bigint,s2::bigint,s3::bigint,s4::bigint,
            c1::bigint,c2::bigint,c3::bigint,
            e1::bigint,e2::bigint,e3::bigint,
            se1::bigint,se2::bigint,se3::bigint,se4::bigint,se5::bigint,
            cc1::bigint,cc2::bigint,cc3::bigint,cc4::bigint,cc5::bigint,
            gc1::bigint,gc2::bigint,gc3::bigint,gc4::bigint,gc5::bigint,
            ad1::bigint,ad2::bigint,ad3::bigint,ad4::bigint,ad5::bigint,ad7::bigint,ad8::bigint,ad9::bigint,
            sr1::bigint,sr2::bigint,sr3::bigint,sr4::bigint,sr5::bigint,sr6::bigint,

            cro13ai_behavior_engagement::bigint,cro13ai_behavior_safety::bigint,cro13ai_behavior_selfesteem::bigint,
            cro13ai_building_a_stronger_community::bigint,cro13ai_building_connect::bigint,cro13ai_classroom_routines::bigint,
            cro13ai_lesson_planning::bigint,cro13ai_look_for_understanding___respond::bigint,cro13ai_mission_buniyad::bigint,
            cro13ai_psychological_safety::bigint,cro13ai_social___emotional_wellbeing::bigint,
            cro13ai_teaching___learning_strategies_1::bigint,cro13ai_teaching___learning_strategies_2::bigint,

            cro13aiii_asking_effective_questions::bigint,cro13aiii_elaborative_questioning::bigint,cro13aiii_emotional_learning_environment::bigint,
            cro13aiii_growth_mindset::bigint,cro13aiii_physical_learning_environment::bigint,cro13aiii_retrieval_practices::bigint,
            cro13aiii_worked_examples::bigint,

            cro13aiv_bridging_covid19_learning_losses::bigint,cro13aiv_classroom_routines::bigint,cro13aiv_longterm_learning::bigint,
            cro13aiv_na::bigint,cro13aiv_socio_emotional_wellbeing::bigint,cro13aiv_structuring_learning_journey::bigint,

            cro13av_growth_mindset::bigint,cro13av_normalising_error::bigint,
            cro13b::bigint,cro13c::bigint
        ] )                                         AS score

    FROM expanded_cro13

),

------------------------------------------------------------------------------
-- Deterministic surrogate key: "KEY" + subindicator  → key_subindicator_key
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
-- Duplicate c1 rows as e3 for dual classification
------------------------------------------------------------------------------
c1_duplicated AS (

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
        'e3' AS subindicator,
        score,
        {{ dbt_utils.generate_surrogate_key(['"KEY"', "'e3'"]) }} AS key_subindicator_key
    FROM with_primary_key
    WHERE subindicator = 'c1'

),

------------------------------------------------------------------------------
-- Union duplicated with original
------------------------------------------------------------------------------
with_duplicates AS (
    SELECT * FROM with_primary_key
    UNION ALL
    SELECT * FROM c1_duplicated
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

            WHEN subindicator IN ('c1', 'c2','c3')
                 THEN 'Curiosity & Critical Thinking'

            WHEN subindicator IN ('e1','e2','e3')
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

    FROM with_duplicates  AS wp
    WHERE score IS NOT NULL
),

------------------------------------------------------------------------------
-- Transform scores for specific subindicators and forms
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
-- Deduplicate: keep the *latest* row per key_subindicator_key
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
-- Final result
------------------------------------------------------------------------------
SELECT *
FROM   deduped
ORDER  BY submissiondate, "KEY"
