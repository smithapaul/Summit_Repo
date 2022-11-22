DROP VIEW CSMRT_OWNER.UM_D_CLASS_EXAM_VW
/

--
-- UM_D_CLASS_EXAM_VW  (View) 
--
CREATE OR REPLACE VIEW CSMRT_OWNER.UM_D_CLASS_EXAM_VW
BEQUEATH DEFINER
AS 
SELECT CRSE_CD,
           CRSE_OFFER_NUM,
           TERM_CD,
           SESSION_CD,
           CLASS_SECTION_CD,
           CLASS_EXAM_SEQ,
           SRC_SYS_ID,
           INSTITUTION_CD,
           CLASS_NUM,
           CLASS_SID,
           FCLTY_SID,
           EXAM_TIME_CODE,
           EXAM_DT,
           EXAM_START_TIME,
           EXAM_END_TIME,
           CLASS_EXAM_TYPE,
           COMBINED_EXAM,
           DATA_ORIGIN,
           CREATED_EW_DTTM,
           LASTUPD_EW_DTTM
      FROM UM_D_CLASS_EXAM
     where DATA_ORIGIN <> 'D'
/
