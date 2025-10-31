CREATE OR REFRESH STREAMING TABLE ccda_observations_gold
TBLPROPERTIES (
  "quality" = "gold",
  "delta.enableChangeDataFeed" = "true",
  "delta.enableDeletionVectors" = "true"
)
COMMENT "Clean, merged Observations from CCDA";

CREATE FLOW ccda_observations_cdc_flow
AS AUTO CDC INTO ccda_observations_gold
FROM (


            -- Query to extract C-CDA title, patient info, all observation data, and file metadata with proper date formatting
            WITH base_data AS (
                SELECT  
                    -- File metadata columns first
                    r.file_name AS file_name,
                    r.file_modification_time AS file_modification_time,
                    r.parsed_xml:title::STRING AS title,
                    r.parsed_xml:recordTarget.patientRole.patient.name.given::STRING AS patient_first_name,
                    r.parsed_xml:recordTarget.patientRole.patient.name.family::STRING AS patient_last_name,
                    r.parsed_xml:recordTarget.patientRole.patient.administrativeGenderCode._code::STRING AS patient_gender,
                    -- Convert birth date from C-CDA format to proper datetime
                    CASE 
                        WHEN length(r.parsed_xml:recordTarget.patientRole.patient.birthTime._value::STRING) >= 8 THEN
                            to_timestamp(r.parsed_xml:recordTarget.patientRole.patient.birthTime._value::STRING, 'yyyyMMddHHmmss')
                        ELSE NULL 
                    END AS patient_dob,
                    r.parsed_xml:recordTarget.patientRole.addr.streetAddressLine::STRING AS patient_street,
                    r.parsed_xml:recordTarget.patientRole.addr.city::STRING AS patient_city,
                    r.parsed_xml:recordTarget.patientRole.addr.state::STRING AS patient_state,
                    r.parsed_xml:recordTarget.patientRole.addr.postalCode::STRING AS patient_zip,
                    -- Convert document date from C-CDA format to proper datetime
                    CASE 
                        WHEN length(r.parsed_xml:effectiveTime._value::STRING) >= 8 THEN
                            to_timestamp(r.parsed_xml:effectiveTime._value::STRING, 'yyyyMMddHHmmss')
                        ELSE NULL 
                    END AS document_date,
                    r.parsed_xml:author.assignedAuthor.representedOrganization.name::STRING AS author_org,
                    r.parsed_xml
                FROM stream(users.satyendranath_sure_2.ccda_variant_silver) r
                WHERE r.parsed_xml IS NOT NULL
            ),

            section_data AS (
                SELECT 
                    b.*,
                    componentExploded.value:section.title::STRING AS section_title,
                    componentExploded.value:section.code._code::STRING AS section_code,
                    componentExploded.value:section.code._displayName::STRING AS section_display_name,
                    componentExploded.value AS section_content
                FROM base_data b,
                LATERAL variant_explode(b.parsed_xml:component.structuredBody.component) AS componentExploded
                WHERE componentExploded.value:section IS NOT NULL
            ),

            -- Extract different types of observations based on actual structure
            vital_signs_data AS (
                SELECT 
                    s.*,
                    'VITAL_SIGNS' AS data_type,
                    obsExploded.value:observation.code._code::STRING AS obs_code,
                    obsExploded.value:observation.code._displayName::STRING AS obs_name,
                    -- Convert observation date from C-CDA format to proper datetime
                    CASE 
                        WHEN length(obsExploded.value:observation.effectiveTime._value::STRING) >= 8 THEN
                            to_timestamp(obsExploded.value:observation.effectiveTime._value::STRING, 'yyyyMMddHHmmss')
                        ELSE NULL 
                    END AS obs_date,
                    obsExploded.value:observation.value._value::STRING AS obs_value,
                    obsExploded.value:observation.value._unit::STRING AS obs_unit,
                    obsExploded.value:observation.statusCode._code::STRING AS obs_status,
                    obsExploded.value:observation.id._root::STRING AS obs_id
                FROM section_data s,
                LATERAL variant_explode(s.section_content:section.entry.organizer.component) AS obsExploded
                WHERE s.section_content:section.entry.organizer IS NOT NULL
                  AND obsExploded.value:observation IS NOT NULL
            ),

            -- Extract lab/report observations  
            lab_data AS (
                SELECT 
                    s.*,
                    'LAB_RESULTS' AS data_type,
                    labObsExploded.value:observation.code._code::STRING AS obs_code,
                    labObsExploded.value:observation.code._displayName::STRING AS obs_name,
                    -- Convert lab observation date from C-CDA format to proper datetime
                    CASE 
                        WHEN length(labObsExploded.value:observation.effectiveTime._value::STRING) >= 8 THEN
                            to_timestamp(labObsExploded.value:observation.effectiveTime._value::STRING, 'yyyyMMddHHmmss')
                        ELSE NULL 
                    END AS obs_date,
                    labObsExploded.value:observation.value._value::STRING AS obs_value,
                    labObsExploded.value:observation.value._unit::STRING AS obs_unit,
                    labObsExploded.value:observation.statusCode._code::STRING AS obs_status,
                    labObsExploded.value:observation.id._root::STRING AS obs_id
                FROM section_data s,
                LATERAL variant_explode(s.section_content:section.entry.organizer.component) AS labObsExploded
                WHERE s.section_code IN ('30954-2', '11502-2', '33747-0') -- Lab results sections
                  AND labObsExploded.value:observation IS NOT NULL
            ),

            -- Extract medication data (substanceAdministration)
            medication_data AS (
                SELECT 
                    s.*,
                    'MEDICATIONS' AS data_type,
                    s.section_content:section.entry.substanceAdministration.consumable.manufacturedProduct.manufacturedMaterial.code._code::STRING AS obs_code,
                    s.section_content:section.entry.substanceAdministration.consumable.manufacturedProduct.manufacturedMaterial.code._displayName::STRING AS obs_name,
                    -- Convert medication date from C-CDA format to proper datetime
                    CASE 
                        WHEN length(s.section_content:section.entry.substanceAdministration.effectiveTime.low._value::STRING) >= 8 THEN
                            to_timestamp(s.section_content:section.entry.substanceAdministration.effectiveTime.low._value::STRING, 'yyyyMMddHHmmss')
                        ELSE NULL 
                    END AS obs_date,
                    s.section_content:section.entry.substanceAdministration.doseQuantity._value::STRING AS obs_value,
                    'dose' AS obs_unit,
                    s.section_content:section.entry.substanceAdministration.statusCode._code::STRING AS obs_status,
                    s.section_content:section.entry.substanceAdministration.id._root::STRING AS obs_id
                FROM section_data s
                WHERE s.section_content:section.entry.substanceAdministration IS NOT NULL
            ),

            -- Extract allergy data
            allergy_data AS (
                SELECT 
                    s.*,
                    'ALLERGIES' AS data_type,
                    s.section_content:section.entry.act.entryRelationship.observation.participant.participantRole.playingEntity.code._code::STRING AS obs_code,
                    s.section_content:section.entry.act.entryRelationship.observation.participant.participantRole.playingEntity.code._displayName::STRING AS obs_name,
                    -- Convert allergy date from C-CDA format to proper datetime
                    CASE 
                        WHEN length(s.section_content:section.entry.act.effectiveTime.low._value::STRING) >= 8 THEN
                            to_timestamp(s.section_content:section.entry.act.effectiveTime.low._value::STRING, 'yyyyMMddHHmmss')
                        ELSE NULL 
                    END AS obs_date,
                    s.section_content:section.entry.act.entryRelationship.observation.value._displayName::STRING AS obs_value,
                    'allergy' AS obs_unit,
                    s.section_content:section.entry.act.statusCode._code::STRING AS obs_status,
                    s.section_content:section.entry.act.id._root::STRING AS obs_id
                FROM section_data s
                WHERE s.section_content:section.entry.act.entryRelationship.observation.participant IS NOT NULL
            )
            SELECT
            file_name,
            file_modification_time,
            title,
            patient_first_name,
            patient_last_name,
            patient_gender,
            patient_dob,
            patient_street,
            patient_city,
            patient_state,
            CASE WHEN patient_zip iLIKE '%_NULLFLAVOR%' THEN NULL ELSE patient_zip END AS patient_zip,
            document_date,
            author_org,
            section_title,
            section_code,
            section_display_name,
            data_type,
            obs_code,
            obs_name,
            obs_date,
            obs_value,
            obs_unit,
            obs_status,
            obs_id,
            current_timestamp() AS insert_date
            from (
                    -- Combine all observation types
                    SELECT 
                        file_name,
                        file_modification_time,
                        title,
                        patient_first_name,
                        patient_last_name,
                        patient_gender,
                        patient_dob,
                        patient_street,
                        patient_city,
                        patient_state,
                        patient_zip,
                        document_date,
                        author_org,
                        section_title,
                        section_code,
                        section_display_name,
                        data_type,
                        obs_code,
                        obs_name,
                        obs_date,
                        obs_value,
                        obs_unit,
                        obs_status,
                        obs_id
                    FROM vital_signs_data
                    WHERE obs_code IS NOT NULL

                    UNION ALL

                    SELECT 
                        file_name,
                        file_modification_time,
                        title,
                        patient_first_name,
                        patient_last_name,
                        patient_gender,
                        patient_dob,
                        patient_street,
                        patient_city,
                        patient_state,
                        patient_zip,
                        document_date,
                        author_org,
                        section_title,
                        section_code,
                        section_display_name,
                        data_type,
                        obs_code,
                        obs_name,
                        obs_date,
                        obs_value,
                        obs_unit,
                        obs_status,
                        obs_id
                    FROM lab_data
                    WHERE obs_code IS NOT NULL

                    UNION ALL

                    SELECT 
                        file_name,
                        file_modification_time,
                        title,
                        patient_first_name,
                        patient_last_name,
                        patient_gender,
                        patient_dob,
                        patient_street,
                        patient_city,
                        patient_state,
                        patient_zip,
                        document_date,
                        author_org,
                        section_title,
                        section_code,
                        section_display_name,
                        data_type,
                        obs_code,
                        obs_name,
                        obs_date,
                        obs_value,
                        obs_unit,
                        obs_status,
                        obs_id
                    FROM medication_data
                    WHERE obs_code IS NOT NULL

                    UNION ALL

                    SELECT 
                        file_name,
                        file_modification_time,
                        title,
                        patient_first_name,
                        patient_last_name,
                        patient_gender,
                        patient_dob,
                        patient_street,
                        patient_city,
                        patient_state,
                        patient_zip,
                        document_date,
                        author_org,
                        section_title,
                        section_code,
                        section_display_name,
                        data_type,
                        obs_code,
                        obs_name,
                        obs_date,
                        obs_value,
                        obs_unit,
                        obs_status,
                        obs_id
                    FROM allergy_data
                    WHERE obs_code IS NOT NULL
            )

)
KEYS (title,document_date,author_org, obs_id)
SEQUENCE BY file_modification_time
STORED AS SCD TYPE 1;