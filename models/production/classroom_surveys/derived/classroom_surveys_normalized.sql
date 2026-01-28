{{ config(
    materialized = 'table'
) }}

------------------------------------------------------------------------------
-- Raw‑to‑long transformation
------------------------------------------------------------------------------
WITH expanded_cro13 AS (
    SELECT *,
        -- Expand cro13ai space-separated values into individual binary indicators
        CASE WHEN cro13ai LIKE '%behavior_engagement%' THEN 1 ELSE 0 END as cro13ai_behavior_engagement_derived,
        CASE WHEN cro13ai LIKE '%behavior_safety%' THEN 1 ELSE 0 END as cro13ai_behavior_safety_derived,
        CASE WHEN cro13ai LIKE '%behavior_selfesteem%' THEN 1 ELSE 0 END as cro13ai_behavior_selfesteem_derived,
        CASE WHEN cro13ai LIKE '%building_a_stronger_community%' THEN 1 ELSE 0 END as cro13ai_building_a_stronger_community_derived,
        CASE WHEN cro13ai LIKE '%building_connect%' THEN 1 ELSE 0 END as cro13ai_building_connect_derived,
        CASE WHEN cro13ai LIKE '%classroom_routines%' THEN 1 ELSE 0 END as cro13ai_classroom_routines_derived,
        CASE WHEN cro13ai LIKE '%lesson_planning%' THEN 1 ELSE 0 END as cro13ai_lesson_planning_derived,
        CASE WHEN cro13ai LIKE '%look_for_understanding%' THEN 1 ELSE 0 END as cro13ai_look_for_understanding___respond_derived,
        CASE WHEN cro13ai LIKE '%mission_buniyad%' THEN 1 ELSE 0 END as cro13ai_mission_buniyad_derived,
        CASE WHEN cro13ai LIKE '%psychological_safety%' THEN 1 ELSE 0 END as cro13ai_psychological_safety_derived,
        CASE WHEN cro13ai LIKE '%social_%emotional_wellbeing%' THEN 1 ELSE 0 END as cro13ai_social___emotional_wellbeing_derived,
        CASE WHEN cro13ai LIKE '%teaching___learning_strategies_1%' THEN 1 ELSE 0 END as cro13ai_teaching___learning_strategies_1_derived,
        CASE WHEN cro13ai LIKE '%teaching___learning_strategies_2%' THEN 1 ELSE 0 END as cro13ai_teaching___learning_strategies_2_derived,

        -- Expand cro13aiii space-separated values
        CASE WHEN cro13aiii LIKE '%asking_effective_questions%' THEN 1 ELSE 0 END as cro13aiii_asking_effective_questions_derived,
        CASE WHEN cro13aiii LIKE '%elaborative_questioning%' THEN 1 ELSE 0 END as cro13aiii_elaborative_questioning_derived,
        CASE WHEN cro13aiii LIKE '%emotional_learning_environment%' THEN 1 ELSE 0 END as cro13aiii_emotional_learning_environment_derived,
        CASE WHEN cro13aiii LIKE '%growth_mindset%' THEN 1 ELSE 0 END as cro13aiii_growth_mindset_derived,
        CASE WHEN cro13aiii LIKE '%physical_learning_environment%' THEN 1 ELSE 0 END as cro13aiii_physical_learning_environment_derived,
        CASE WHEN cro13aiii LIKE '%retrieval_practices%' THEN 1 ELSE 0 END as cro13aiii_retrieval_practices_derived,
        CASE WHEN cro13aiii LIKE '%worked_examples%' THEN 1 ELSE 0 END as cro13aiii_worked_examples_derived,

        -- Expand cro13aiv space-separated values
        CASE WHEN cro13aiv LIKE '%bridging_covid19_learning_losses%' THEN 1 ELSE 0 END as cro13aiv_bridging_covid19_learning_losses_derived,
        CASE WHEN cro13aiv LIKE '%classroom_routines%' THEN 1 ELSE 0 END as cro13aiv_classroom_routines_derived,
        CASE WHEN cro13aiv LIKE '%longterm_learning%' THEN 1 ELSE 0 END as cro13aiv_longterm_learning_derived,
        CASE WHEN cro13aiv LIKE '%na%' THEN 1 ELSE 0 END as cro13aiv_na_derived,
        CASE WHEN cro13aiv LIKE '%socio_emotional_wellbeing%' THEN 1 ELSE 0 END as cro13aiv_socio_emotional_wellbeing_derived,
        CASE WHEN cro13aiv LIKE '%structuring_learning_journey%' THEN 1 ELSE 0 END as cro13aiv_structuring_learning_journey_derived,

        -- Expand cro13av space-separated values
        CASE WHEN cro13av LIKE '%growth_mindset%' THEN 1 ELSE 0 END as cro13av_growth_mindset_derived,
        CASE WHEN cro13av LIKE '%normalising_error%' THEN 1 ELSE 0 END as cro13av_normalising_error_derived

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
            CASE WHEN s1::text ~ '^-?\\d+$' THEN s1::bigint END,
            CASE WHEN s2::text ~ '^-?\\d+$' THEN s2::bigint END,
            CASE WHEN s3::text ~ '^-?\\d+$' THEN s3::bigint END,
            CASE WHEN s4::text ~ '^-?\\d+$' THEN s4::bigint END,

            CASE WHEN c1::text ~ '^-?\\d+$' THEN c1::bigint END,
            CASE WHEN c2::text ~ '^-?\\d+$' THEN c2::bigint END,
            CASE WHEN c3::text ~ '^-?\\d+$' THEN c3::bigint END,

            CASE WHEN e1::text ~ '^-?\\d+$' THEN e1::bigint END,
            CASE WHEN e2::text ~ '^-?\\d+$' THEN e2::bigint END,
            CASE WHEN e3::text ~ '^-?\\d+$' THEN e3::bigint END,

            CASE WHEN se1::text ~ '^-?\\d+$' THEN se1::bigint END,
            CASE WHEN se2::text ~ '^-?\\d+$' THEN se2::bigint END,
            CASE WHEN se3::text ~ '^-?\\d+$' THEN se3::bigint END,
            CASE WHEN se4::text ~ '^-?\\d+$' THEN se4::bigint END,
            CASE WHEN se5::text ~ '^-?\\d+$' THEN se5::bigint END,

            CASE WHEN cc1::text ~ '^-?\\d+$' THEN cc1::bigint END,
            CASE WHEN cc2::text ~ '^-?\\d+$' THEN cc2::bigint END,
            CASE WHEN cc3::text ~ '^-?\\d+$' THEN cc3::bigint END,
            CASE WHEN cc4::text ~ '^-?\\d+$' THEN cc4::bigint END,
            CASE WHEN cc5::text ~ '^-?\\d+$' THEN cc5::bigint END,

            CASE WHEN gc1::text ~ '^-?\\d+$' THEN gc1::bigint END,
            CASE WHEN gc2::text ~ '^-?\\d+$' THEN gc2::bigint END,
            CASE WHEN gc3::text ~ '^-?\\d+$' THEN gc3::bigint END,
            CASE WHEN gc4::text ~ '^-?\\d+$' THEN gc4::bigint END,
            CASE WHEN gc5::text ~ '^-?\\d+$' THEN gc5::bigint END,

            CASE WHEN ad1::text ~ '^-?\\d+$' THEN ad1::bigint END,
            CASE WHEN ad2::text ~ '^-?\\d+$' THEN ad2::bigint END,
            CASE WHEN ad3::text ~ '^-?\\d+$' THEN ad3::bigint END,
            CASE WHEN ad4::text ~ '^-?\\d+$' THEN ad4::bigint END,
            CASE WHEN ad5::text ~ '^-?\\d+$' THEN ad5::bigint END,
            CASE WHEN ad7::text ~ '^-?\\d+$' THEN ad7::bigint END,
            CASE WHEN ad8::text ~ '^-?\\d+$' THEN ad8::bigint END,
            CASE WHEN ad9::text ~ '^-?\\d+$' THEN ad9::bigint END,

            CASE WHEN sr1::text ~ '^-?\\d+$' THEN sr1::bigint END,
            CASE WHEN sr2::text ~ '^-?\\d+$' THEN sr2::bigint END,
            CASE WHEN sr3::text ~ '^-?\\d+$' THEN sr3::bigint END,
            CASE WHEN sr4::text ~ '^-?\\d+$' THEN sr4::bigint END,
            CASE WHEN sr5::text ~ '^-?\\d+$' THEN sr5::bigint END,
            CASE WHEN sr6::text ~ '^-?\\d+$' THEN sr6::bigint END,

            cro13ai_behavior_engagement_derived::bigint,
            cro13ai_behavior_safety_derived::bigint,
            cro13ai_behavior_selfesteem_derived::bigint,
            cro13ai_building_a_stronger_community_derived::bigint,
            cro13ai_building_connect_derived::bigint,
            cro13ai_classroom_routines_derived::bigint,
            cro13ai_lesson_planning_derived::bigint,
            cro13ai_look_for_understanding___respond_derived::bigint,
            cro13ai_mission_buniyad_derived::bigint,
            cro13ai_psychological_safety_derived::bigint,
            cro13ai_social___emotional_wellbeing_derived::bigint,
            cro13ai_teaching___learning_strategies_1_derived::bigint,
            cro13ai_teaching___learning_strategies_2_derived::bigint,

            cro13aiii_asking_effective_questions_derived::bigint,
            cro13aiii_elaborative_questioning_derived::bigint,
            cro13aiii_emotional_learning_environment_derived::bigint,
            cro13aiii_growth_mindset_derived::bigint,
            cro13aiii_physical_learning_environment_derived::bigint,
            cro13aiii_retrieval_practices_derived::bigint,
            cro13aiii_worked_examples_derived::bigint,

            cro13aiv_bridging_covid19_learning_losses_derived::bigint,
            cro13aiv_classroom_routines_derived::bigint,
            cro13aiv_longterm_learning_derived::bigint,
            cro13aiv_na_derived::bigint,
            cro13aiv_socio_emotional_wellbeing_derived::bigint,
            cro13aiv_structuring_learning_journey_derived::bigint,

            cro13av_growth_mindset_derived::bigint,
            cro13av_normalising_error_derived::bigint,
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
