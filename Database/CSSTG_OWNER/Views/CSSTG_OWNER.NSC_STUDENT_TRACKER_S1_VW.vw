DROP VIEW CSSTG_OWNER.NSC_STUDENT_TRACKER_S1_VW
/

--
-- NSC_STUDENT_TRACKER_S1_VW  (View) 
--
CREATE OR REPLACE VIEW CSSTG_OWNER.NSC_STUDENT_TRACKER_S1_VW
BEQUEATH DEFINER
AS 
SELECT UNIQUE_ID,
           FIRST_NAME,
           MIDDLE_INITIAL,
           LAST_NAME,
           SUFFIX,
           RETURN_FIELD,
           RECORD_FOUND,
           SEARCH_DATE,
           COLLEGE_ID,
           COLLEGE_NAME,
           COLLEGE_STATE,
           YEAR_2_4,
           PUB_PRIVATE,
           ENROLL_BEGIN_DATE,
           ENROLL_END_DATE,
           ENROLL_STATUS,
           CLASS_LEVEL,
           ENROLL_MAJOR_1,
           ENROLL_CIP_1,
           ENROLL_MAJOR_2,
           ENROLL_CIP_2,
           GRADUATED_FLG,
           GRAD_DATE,
           DEGREE_TITLE,
           DEG_MAJOR_1,
           DEG_CIP_1,
           DEG_MAJOR_2,
           DEG_CIP_2,
           DEG_MAJOR_3,
           DEG_CIP_3,
           DEG_MAJOR_4,
           DEG_CIP_4,
           COLLEGE_SEQ_NBR,
           INSTITUTION_CD,
           PERSON_ID,
           APPL_NBR,
           TERM_CD,
           SEARCH_TYPE,
           FILE_NAME,
           FILE_DATE,
           RECORD_NUMBER,
           INSERT_TIME
--      FROM CSSTG_OWNER.NSC_STUDENT_TRACKER_S1@SMTPROD
      FROM CSSTG_OWNER.NSC_STUDENT_TRACKER_S1@SMTDEV        -- Temp until APEX app moved to prod.
/
