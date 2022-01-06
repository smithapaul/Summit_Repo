CREATE OR REPLACE VIEW C_UM_R_STDNT_ADVR_VW3
BEQUEATH DEFINER
AS 
WITH Q1 AS
      (SELECT /*+ parallel(8) inline */ INSTITUTION_SID,
              PERSON_SID,
              ADVISOR_ROLE,
              STDNT_ADVISOR_NBR,
              SRC_SYS_ID,
              INSTITUTION_CD,
              PERSON_ID          EMPLID,
              EFFDT,
              STUDENT_ADVISOR_SID,
              ACAD_CAR_SID,
              ACAD_PROG_SID,
              ACAD_PLAN_SID,
              APPROVE_ENRLMT,
              APPROVE_GRAD,
              GRAD_APPROVED,
              COMMITTEE_ID,
              COMM_PERS_CD,
              ADVISOR_ROLE_LD,
              ADVISOR_ROLE_SD,
              STUDENT_ADVISOR_NM,
              STUDENT_ADVISOR_ORDER,
              cast('N' as VARCHAR2(1))         LOAD_ERROR,
              DATA_ORIGIN,
              CREATED_EW_DTTM,
              LASTUPD_EW_DTTM,
              '1234'                        BATCH_SID,
              row_number() over (partition by INSTITUTION_CD, PERSON_ID, ADVISOR_ROLE, 
			                                  STDNT_ADVISOR_NBR, SRC_SYS_ID
                                     order by TERM_CD desc, ACAD_CAR_CD desc, ACAD_PROG_CD, ACAD_PLAN_CD, ACAD_SPLAN_CD) Q_ORDER  									 
--NK =  TERM_CD, PERSON_ID, STDNT_CAR_NUM, ACAD_PLAN_CD, ACAD_SPLAN_CD, ADVISOR_ROLE, STDNT_ADVISOR_NBR
  FROM CSMRT_OWNER.UM_F_STDNT_ADVR  
 WHERE DATA_ORIGIN <> 'D'
   --AND PERSON_ID = '00915783'
)
  SELECT INSTITUTION_SID,
       PERSON_SID,
       ADVISOR_ROLE,
       STDNT_ADVISOR_NBR,
       SRC_SYS_ID,
       INSTITUTION_CD,
       EMPLID,
       EFFDT,
       STUDENT_ADVISOR_SID,
       ACAD_CAR_SID,
       ACAD_PROG_SID,
       ACAD_PLAN_SID,
       APPROVE_ENRLMT,
       APPROVE_GRAD,
       GRAD_APPROVED,
       COMMITTEE_ID,
       COMM_PERS_CD,
       ADVISOR_ROLE_LD,
       ADVISOR_ROLE_SD,
       STUDENT_ADVISOR_NM,
       STUDENT_ADVISOR_ORDER,
       LOAD_ERROR,
       DATA_ORIGIN,
       CREATED_EW_DTTM,
       LASTUPD_EW_DTTM,
       BATCH_SID
  FROM Q1 
 WHERE Q_ORDER = 1;
