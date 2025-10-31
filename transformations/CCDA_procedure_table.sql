CREATE OR REFRESH STREAMING TABLE ccda_procedure_gold
TBLPROPERTIES (
  "quality" = "gold",
  "delta.enableChangeDataFeed" = "true",
  "delta.enableDeletionVectors" = "true"
)
COMMENT "Clean, merged procedure from CCDA";

CREATE FLOW ccda_procedure_cdc_flow
AS AUTO CDC INTO ccda_procedure_gold
FROM (


                    -- Query to extract C-CDA title, patient info, all procedure data, and file metadata with proper date formatting
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
                            CASE
                            WHEN typeof(r.parsed_xml:recordTarget.patientRole.addr.postalCode) = 'OBJECT'
                            AND r.parsed_xml:recordTarget.patientRole.addr.postalCode._nullFlavor IS NOT NULL
                            THEN NULL
                            ELSE upper(trim(CAST(r.parsed_xml:recordTarget.patientRole.addr.postalCode AS STRING)))
                            END AS patient_zip,
                            -- r.parsed_xml:recordTarget.patientRole.addr.postalCode::STRING AS patient_zip,
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

                    -- Extract procedures from sections with direct procedure entries
                    direct_procedure_data AS (
                        SELECT 
                            s.*,
                            'DIRECT_PROCEDURE' AS data_type,
                            s.section_content:section.entry.procedure.code._code::STRING AS proc_code,
                            s.section_content:section.entry.procedure.code._displayName::STRING AS proc_name,
                            s.section_content:section.entry.procedure.code._codeSystem::STRING AS proc_code_system,
                            -- Convert procedure date from C-CDA format to proper datetime
                            CASE 
                                WHEN length(s.section_content:section.entry.procedure.effectiveTime._value::STRING) >= 8 THEN
                                    to_timestamp(s.section_content:section.entry.procedure.effectiveTime._value::STRING, 'yyyyMMddHHmmss')
                                ELSE NULL 
                            END AS proc_date,
                            s.section_content:section.entry.procedure.statusCode._code::STRING AS proc_status,
                            s.section_content:section.entry.procedure.id._root::STRING AS proc_id,
                            s.section_content:section.entry.procedure.text._reference::STRING AS proc_text_ref,
                            s.section_content:section.entry.procedure._classCode::STRING AS proc_class,
                            s.section_content:section.entry.procedure._moodCode::STRING AS proc_mood,
                            s.section_content:section.entry.procedure.templateId._root::STRING AS proc_template_id
                        FROM section_data s
                        WHERE s.section_content:section.entry.procedure IS NOT NULL
                    ),

                    -- Extract procedures from sections that might have multiple entries
                    multi_entry_procedure_data AS (
                        SELECT 
                            s.*,
                            'MULTI_ENTRY_PROCEDURE' AS data_type,
                            entryExploded.value:procedure.code._code::STRING AS proc_code,
                            entryExploded.value:procedure.code._displayName::STRING AS proc_name,
                            entryExploded.value:procedure.code._codeSystem::STRING AS proc_code_system,
                            -- Convert procedure date from C-CDA format to proper datetime
                            CASE 
                                WHEN length(entryExploded.value:procedure.effectiveTime._value::STRING) >= 8 THEN
                                    to_timestamp(entryExploded.value:procedure.effectiveTime._value::STRING, 'yyyyMMddHHmmss')
                                ELSE NULL 
                            END AS proc_date,
                            entryExploded.value:procedure.statusCode._code::STRING AS proc_status,
                            entryExploded.value:procedure.id._root::STRING AS proc_id,
                            entryExploded.value:procedure.text._reference::STRING AS proc_text_ref,
                            entryExploded.value:procedure._classCode::STRING AS proc_class,
                            entryExploded.value:procedure._moodCode::STRING AS proc_mood,
                            entryExploded.value:procedure.templateId._root::STRING AS proc_template_id
                        FROM section_data s,
                        LATERAL variant_explode(s.section_content:section.entry) AS entryExploded
                        WHERE entryExploded.value:procedure IS NOT NULL
                    ),

                    -- Extract surgical procedures specifically (if they exist as a separate section)
                    surgical_procedure_data AS (
                        SELECT 
                            s.*,
                            'SURGICAL_PROCEDURE' AS data_type,
                            s.section_content:section.entry.procedure.code._code::STRING AS proc_code,
                            s.section_content:section.entry.procedure.code._displayName::STRING AS proc_name,
                            s.section_content:section.entry.procedure.code._codeSystem::STRING AS proc_code_system,
                            -- Convert surgical procedure date from C-CDA format to proper datetime
                            CASE 
                                WHEN length(s.section_content:section.entry.procedure.effectiveTime._value::STRING) >= 8 THEN
                                    to_timestamp(s.section_content:section.entry.procedure.effectiveTime._value::STRING, 'yyyyMMddHHmmss')
                                ELSE NULL 
                            END AS proc_date,
                            s.section_content:section.entry.procedure.statusCode._code::STRING AS proc_status,
                            s.section_content:section.entry.procedure.id._root::STRING AS proc_id,
                            s.section_content:section.entry.procedure.text._reference::STRING AS proc_text_ref,
                            s.section_content:section.entry.procedure._classCode::STRING AS proc_class,
                            s.section_content:section.entry.procedure._moodCode::STRING AS proc_mood,
                            s.section_content:section.entry.procedure.templateId._root::STRING AS proc_template_id
                        FROM section_data s
                        WHERE s.section_code = '47519-4' -- "List of surgeries" section
                        AND s.section_content:section.entry.procedure IS NOT NULL
                    )

                    -- Combine all procedure types
                    SELECT file_name,
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
                        proc_code,
                        proc_name,
                        proc_code_system,
                        proc_date,
                        proc_status,
                        proc_id,
                        proc_text_ref,
                        proc_class,
                        proc_mood,
                        proc_template_id
                        FROM (
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
                                proc_code,
                                proc_name,
                                proc_code_system,
                                proc_date,
                                proc_status,
                                proc_id,
                                proc_text_ref,
                                proc_class,
                                proc_mood,
                                proc_template_id
                            FROM direct_procedure_data
                            WHERE proc_code IS NOT NULL

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
                                proc_code,
                                proc_name,
                                proc_code_system,
                                proc_date,
                                proc_status,
                                proc_id,
                                proc_text_ref,
                                proc_class,
                                proc_mood,
                                proc_template_id
                            FROM multi_entry_procedure_data
                            WHERE proc_code IS NOT NULL

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
                                proc_code,
                                proc_name,
                                proc_code_system,
                                proc_date,
                                proc_status,
                                proc_id,
                                proc_text_ref,
                                proc_class,
                                proc_mood,
                                proc_template_id
                            FROM surgical_procedure_data
                            WHERE proc_code IS NOT NULL
                        ) 



)
KEYS (title,document_date,author_org, proc_id)
SEQUENCE BY file_modification_time
STORED AS SCD TYPE 1;