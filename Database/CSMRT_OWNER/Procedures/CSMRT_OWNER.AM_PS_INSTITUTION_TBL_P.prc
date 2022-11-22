DROP PROCEDURE CSMRT_OWNER.AM_PS_INSTITUTION_TBL_P
/

--
-- AM_PS_INSTITUTION_TBL_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."AM_PS_INSTITUTION_TBL_P" IS

   ------------------------------------------------------------------------
   -- George Adams
   --
   -- Loads stage table PS_INSTITUTION_TBL from PeopleSoft table PS_INSTITUTION_TBL.
   --
   -- V01  SMT-xxxx 07/10/2017,    Preethi Lodha
   --                              Converted from PS_INSTITUTION_TBL.SQL
   --
   ------------------------------------------------------------------------

   strMartId          VARCHAR2 (50) := 'CSW';
   strProcessName     VARCHAR2 (100) := 'PS_INSTITUTION_TBL';
   intProcessSid      INTEGER;
   dtProcessStart     DATE := SYSDATE;
   strMessage01       VARCHAR2 (4000);
   strMessage02       VARCHAR2 (512);
   strMessage03       VARCHAR2 (512) := '';
   strNewLine         VARCHAR2 (2) := CHR (13) || CHR (10);
   strSqlCommand      VARCHAR2 (32767) := '';
   strSqlDynamic      VARCHAR2 (32767) := '';
   strClientInfo      VARCHAR2 (100);
   intRowCount        INTEGER;
   intTotalRowCount   INTEGER := 0;
   numSqlCode         NUMBER;
   strSqlErrm         VARCHAR2 (4000);
   intTries           INTEGER;
BEGIN
   strSqlCommand := 'DBMS_APPLICATION_INFO.SET_CLIENT_INFO';
   DBMS_APPLICATION_INFO.SET_CLIENT_INFO (strProcessName);

   strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_INIT';
   COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_INIT (
      i_MartId             => strMartId,
      i_ProcessName        => strProcessName,
      i_ProcessStartTime   => dtProcessStart,
      o_ProcessSid         => intProcessSid);

   strMessage01 := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);


   strSqlCommand := 'update START_DT on AMSTG_OWNER.UM_STAGE_JOBS';

   UPDATE AMSTG_OWNER.UM_STAGE_JOBS
      SET TABLE_STATUS = 'Reading', START_DT = SYSDATE, END_DT = NULL
    WHERE TABLE_NAME = 'PS_INSTITUTION_TBL';

   strSqlCommand := 'commit';
   COMMIT;


   strSqlCommand := 'update NEW_MAX_SCN on AMSTG_OWNER.UM_STAGE_JOBS';

   UPDATE AMSTG_OWNER.UM_STAGE_JOBS
      SET TABLE_STATUS = 'Merging',
          NEW_MAX_SCN =
             (SELECT /*+ full(S) */
                    MAX (ORA_ROWSCN)
                FROM SYSADM.PS_INSTITUTION_TBL@AMSOURCE S)
    WHERE TABLE_NAME = 'PS_INSTITUTION_TBL';

   strSqlCommand := 'commit';
   COMMIT;


   strMessage01 := 'Merging data into AMSTG_OWNER.PS_INSTITUTION_TBL';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'merge into AMSTG_OWNER.PS_INSTITUTION_TBL';

   MERGE /*+ use_hash(S,T) */
        INTO  AMSTG_OWNER.PS_INSTITUTION_TBL T
        USING (SELECT /*+ full(S) */
                     NVL (TRIM (INSTITUTION), '-') INSTITUTION,
                      TO_DATE (
                         TO_CHAR (
                            CASE
                               WHEN EFFDT < '01-JAN-1800' THEN NULL
                               ELSE EFFDT
                            END,
                            'MM/DD/YYYY HH24:MI:SS'),
                         'MM/DD/YYYY HH24:MI:SS')
                         EFFDT,
                      NVL (TRIM (EFF_STATUS), '-') EFF_STATUS,
                      NVL (TRIM (DESCR), '-') DESCR,
                      NVL (TRIM (DESCRSHORT), '-') DESCRSHORT,
                      NVL (TRIM (DESCRFORMAL), '-') DESCRFORMAL,
                      NVL (TRIM (COUNTRY), '-') COUNTRY,
                      NVL (TRIM (ADDRESS1), '-') ADDRESS1,
                      NVL (TRIM (ADDRESS2), '-') ADDRESS2,
                      NVL (TRIM (ADDRESS3), '-') ADDRESS3,
                      NVL (TRIM (ADDRESS4), '-') ADDRESS4,
                      NVL (TRIM (CITY), '-') CITY,
                      NVL (TRIM (NUM1), '-') NUM1,
                      NVL (TRIM (NUM2), '-') NUM2,
                      NVL (TRIM (HOUSE_TYPE), '-') HOUSE_TYPE,
                      NVL (TRIM (ADDR_FIELD1), '-') ADDR_FIELD1,
                      NVL (TRIM (ADDR_FIELD2), '-') ADDR_FIELD2,
                      NVL (TRIM (ADDR_FIELD3), '-') ADDR_FIELD3,
                      NVL (TRIM (COUNTY), '-') COUNTY,
                      NVL (TRIM (STATE), '-') STATE,
                      NVL (TRIM (POSTAL), '-') POSTAL,
                      NVL (TRIM (GEO_CODE), '-') GEO_CODE,
                      NVL (TRIM (IN_CITY_LIMIT), '-') IN_CITY_LIMIT,
                      NVL (TRIM (GRADING_SCHEME), '-') GRADING_SCHEME,
                      NVL (TRIM (GRADING_BASIS), '-') GRADING_BASIS,
                      NVL (TRIM (GRADING_BASIS_SCH), '-') GRADING_BASIS_SCH,
                      NVL (TRIM (CAMPUS), '-') CAMPUS,
                      NVL (TRIM (STDNT_SPEC_PERM), '-') STDNT_SPEC_PERM,
                      NVL (TRIM (AUTO_ENRL_WAITLIST), '-') AUTO_ENRL_WAITLIST,
                      NVL (TRIM (RESIDENCY_REQ), '-') RESIDENCY_REQ,
                      NVL (TRIM (FA_WDCAN_RSN), '-') FA_WDCAN_RSN,
                      NVL (TRIM (ENRL_ACTION_REASON), '-') ENRL_ACTION_REASON,
                      NVL (TRIM (FACILITY_CONFLICT), '-') FACILITY_CONFLICT,
                      NVL (TRIM (NSLC_AGD_RULE), '-') NSLC_AGD_RULE,
                      NVL (NSLC_MONTH_FACTOR, 0) NSLC_MONTH_FACTOR,
                      NVL (TRIM (STDNT_ATTR_COHORT), '-') STDNT_ATTR_COHORT,
                      NVL (TRIM (CLASS_MTG_ATND_TYP), '-') CLASS_MTG_ATND_TYP,
                      NVL (TRIM (FICE_CD), '-') FICE_CD,
                      NVL (TRIM (LOAD_CALC_APPLY), '-') LOAD_CALC_APPLY,
                      NVL (FULLTIME_LIMIT_PCT, 0) FULLTIME_LIMIT_PCT,
                      NVL (FULLTIM_LIMIT_WARN, 0) FULLTIM_LIMIT_WARN,
                      NVL (PARTTIME_LIMIT_PCT, 0) PARTTIME_LIMIT_PCT,
                      NVL (PARTTIM_LIMIT_WARN, 0) PARTTIM_LIMIT_WARN,
                      NVL (TRIM (ASSIGN_TYPE), '-') ASSIGN_TYPE,
                      NVL (TRIM (INSTRUCTOR_CLASS), '-') INSTRUCTOR_CLASS,
                      NVL (CRSE_CNTCT_HRS_PCT, 0) CRSE_CNTCT_HRS_PCT,
                      NVL (UNITS_ACAD_PRG_PCT, 0) UNITS_ACAD_PRG_PCT,
                      NVL (TRIM (LMS_FILE_TYPE), '-') LMS_FILE_TYPE,
                      NVL (TRIM (PHONE_TYPE), '-') PHONE_TYPE,
                      NVL (TRIM (ADDR_USAGE), '-') ADDR_USAGE,
                      NVL (TRIM (REPEAT_ENRL_CTL), '-') REPEAT_ENRL_CTL,
                      NVL (TRIM (REPEAT_ENRL_SUSP), '-') REPEAT_ENRL_SUSP,
                      NVL (TRIM (REPEAT_GRD_CK), '-') REPEAT_GRD_CK,
                      NVL (TRIM (REPEAT_GRD_SUSP), '-') REPEAT_GRD_SUSP,
                      NVL (TRIM (GRAD_NAME_CHG), '-') GRAD_NAME_CHG,
                      NVL (TRIM (PRINT_NID), '-') PRINT_NID,
                      NVL (TRIM (REPEAT_CHK_TOPIC), '-') REPEAT_CHK_TOPIC,
                      NVL (TRIM (SCC_AUS_DEST), '-') SCC_AUS_DEST,
                      NVL (TRIM (SCC_CAN_GOV_RPT), '-') SCC_CAN_GOV_RPT,
                      NVL (TRIM (SCC_NZL_ENR), '-') SCC_NZL_ENR,
                      NVL (TRIM (SCC_NZL_NZQA), '-') SCC_NZL_NZQA,
                      NVL (TRIM (SSR_USE_WEEKS), '-') SSR_USE_WEEKS,
                      NVL (TRIM (SSR_ENBL_ACAD_PROG), '-') SSR_ENBL_ACAD_PROG,
                      NVL (TRIM (SSR_CLASS_CANC_ENR), '-') SSR_CLASS_CANC_ENR,
                      NVL (TRIM (SSR_CLASS_CANC_NON), '-') SSR_CLASS_CANC_NON,
                      NVL (TRIM (EXT_USERID_OPT), '-') EXT_USERID_OPT,
                      NVL (TRIM (LMS_PROVIDER), '-') LMS_PROVIDER,
                      NVL (TRIM (E_ADDR_TYPE), '-') E_ADDR_TYPE,
                      NVL (TRIM (SCC_HE_USED_NLD), '-') SCC_HE_USED_NLD
                 FROM SYSADM.PS_INSTITUTION_TBL@AMSOURCE S
                WHERE ORA_ROWSCN > (SELECT OLD_MAX_SCN
                                      FROM AMSTG_OWNER.UM_STAGE_JOBS
                                     WHERE TABLE_NAME = 'PS_INSTITUTION_TBL')) S
           ON (    T.INSTITUTION = S.INSTITUTION
               AND T.EFFDT = S.EFFDT
               AND T.SRC_SYS_ID = 'CS90')
   WHEN MATCHED
   THEN
      UPDATE SET
         T.EFF_STATUS = S.EFF_STATUS,
         T.DESCR = S.DESCR,
         T.DESCRSHORT = S.DESCRSHORT,
         T.DESCRFORMAL = S.DESCRFORMAL,
         T.COUNTRY = S.COUNTRY,
         T.ADDRESS1 = S.ADDRESS1,
         T.ADDRESS2 = S.ADDRESS2,
         T.ADDRESS3 = S.ADDRESS3,
         T.ADDRESS4 = S.ADDRESS4,
         T.CITY = S.CITY,
         T.NUM1 = S.NUM1,
         T.NUM2 = S.NUM2,
         T.HOUSE_TYPE = S.HOUSE_TYPE,
         T.ADDR_FIELD1 = S.ADDR_FIELD1,
         T.ADDR_FIELD2 = S.ADDR_FIELD2,
         T.ADDR_FIELD3 = S.ADDR_FIELD3,
         T.COUNTY = S.COUNTY,
         T.STATE = S.STATE,
         T.POSTAL = S.POSTAL,
         T.GEO_CODE = S.GEO_CODE,
         T.IN_CITY_LIMIT = S.IN_CITY_LIMIT,
         T.GRADING_SCHEME = S.GRADING_SCHEME,
         T.GRADING_BASIS = S.GRADING_BASIS,
         T.GRADING_BASIS_SCH = S.GRADING_BASIS_SCH,
         T.CAMPUS = S.CAMPUS,
         T.STDNT_SPEC_PERM = S.STDNT_SPEC_PERM,
         T.AUTO_ENRL_WAITLIST = S.AUTO_ENRL_WAITLIST,
         T.RESIDENCY_REQ = S.RESIDENCY_REQ,
         T.FA_WDCAN_RSN = S.FA_WDCAN_RSN,
         T.ENRL_ACTION_REASON = S.ENRL_ACTION_REASON,
         T.FACILITY_CONFLICT = S.FACILITY_CONFLICT,
         T.NSLC_AGD_RULE = S.NSLC_AGD_RULE,
         T.NSLC_MONTH_FACTOR = S.NSLC_MONTH_FACTOR,
         T.STDNT_ATTR_COHORT = S.STDNT_ATTR_COHORT,
         T.CLASS_MTG_ATND_TYP = S.CLASS_MTG_ATND_TYP,
         T.FICE_CD = S.FICE_CD,
         T.LOAD_CALC_APPLY = S.LOAD_CALC_APPLY,
         T.FULLTIME_LIMIT_PCT = S.FULLTIME_LIMIT_PCT,
         T.FULLTIM_LIMIT_WARN = S.FULLTIM_LIMIT_WARN,
         T.PARTTIME_LIMIT_PCT = S.PARTTIME_LIMIT_PCT,
         T.PARTTIM_LIMIT_WARN = S.PARTTIM_LIMIT_WARN,
         T.ASSIGN_TYPE = S.ASSIGN_TYPE,
         T.INSTRUCTOR_CLASS = S.INSTRUCTOR_CLASS,
         T.CRSE_CNTCT_HRS_PCT = S.CRSE_CNTCT_HRS_PCT,
         T.UNITS_ACAD_PRG_PCT = S.UNITS_ACAD_PRG_PCT,
         T.LMS_FILE_TYPE = S.LMS_FILE_TYPE,
         T.PHONE_TYPE = S.PHONE_TYPE,
         T.ADDR_USAGE = S.ADDR_USAGE,
         T.REPEAT_ENRL_CTL = S.REPEAT_ENRL_CTL,
         T.REPEAT_ENRL_SUSP = S.REPEAT_ENRL_SUSP,
         T.REPEAT_GRD_CK = S.REPEAT_GRD_CK,
         T.REPEAT_GRD_SUSP = S.REPEAT_GRD_SUSP,
         T.GRAD_NAME_CHG = S.GRAD_NAME_CHG,
         T.PRINT_NID = S.PRINT_NID,
         T.REPEAT_CHK_TOPIC = S.REPEAT_CHK_TOPIC,
         T.SCC_AUS_DEST = S.SCC_AUS_DEST,
         T.SCC_CAN_GOV_RPT = S.SCC_CAN_GOV_RPT,
         T.SCC_NZL_ENR = S.SCC_NZL_ENR,
         T.SCC_NZL_NZQA = S.SCC_NZL_NZQA,
         T.SSR_USE_WEEKS = S.SSR_USE_WEEKS,
         T.SSR_ENBL_ACAD_PROG = S.SSR_ENBL_ACAD_PROG,
         T.SSR_CLASS_CANC_ENR = S.SSR_CLASS_CANC_ENR,
         T.SSR_CLASS_CANC_NON = S.SSR_CLASS_CANC_NON,
         T.EXT_USERID_OPT = S.EXT_USERID_OPT,
         T.LMS_PROVIDER = S.LMS_PROVIDER,
         T.E_ADDR_TYPE = S.E_ADDR_TYPE,
         T.SCC_HE_USED_NLD = S.SCC_HE_USED_NLD,
         T.DATA_ORIGIN = 'S',
         T.LASTUPD_EW_DTTM = SYSDATE,
         T.BATCH_SID = 1234
              WHERE    T.EFF_STATUS <> S.EFF_STATUS
                    OR T.DESCR <> S.DESCR
                    OR T.DESCRSHORT <> S.DESCRSHORT
                    OR T.DESCRFORMAL <> S.DESCRFORMAL
                    OR T.COUNTRY <> S.COUNTRY
                    OR T.ADDRESS1 <> S.ADDRESS1
                    OR T.ADDRESS2 <> S.ADDRESS2
                    OR T.ADDRESS3 <> S.ADDRESS3
                    OR T.ADDRESS4 <> S.ADDRESS4
                    OR T.CITY <> S.CITY
                    OR T.NUM1 <> S.NUM1
                    OR T.NUM2 <> S.NUM2
                    OR T.HOUSE_TYPE <> S.HOUSE_TYPE
                    OR T.ADDR_FIELD1 <> S.ADDR_FIELD1
                    OR T.ADDR_FIELD2 <> S.ADDR_FIELD2
                    OR T.ADDR_FIELD3 <> S.ADDR_FIELD3
                    OR T.COUNTY <> S.COUNTY
                    OR T.STATE <> S.STATE
                    OR T.POSTAL <> S.POSTAL
                    OR T.GEO_CODE <> S.GEO_CODE
                    OR T.IN_CITY_LIMIT <> S.IN_CITY_LIMIT
                    OR T.GRADING_SCHEME <> S.GRADING_SCHEME
                    OR T.GRADING_BASIS <> S.GRADING_BASIS
                    OR T.GRADING_BASIS_SCH <> S.GRADING_BASIS_SCH
                    OR T.CAMPUS <> S.CAMPUS
                    OR T.STDNT_SPEC_PERM <> S.STDNT_SPEC_PERM
                    OR T.AUTO_ENRL_WAITLIST <> S.AUTO_ENRL_WAITLIST
                    OR T.RESIDENCY_REQ <> S.RESIDENCY_REQ
                    OR T.FA_WDCAN_RSN <> S.FA_WDCAN_RSN
                    OR T.ENRL_ACTION_REASON <> S.ENRL_ACTION_REASON
                    OR T.FACILITY_CONFLICT <> S.FACILITY_CONFLICT
                    OR T.NSLC_AGD_RULE <> S.NSLC_AGD_RULE
                    OR T.NSLC_MONTH_FACTOR <> S.NSLC_MONTH_FACTOR
                    OR T.STDNT_ATTR_COHORT <> S.STDNT_ATTR_COHORT
                    OR T.CLASS_MTG_ATND_TYP <> S.CLASS_MTG_ATND_TYP
                    OR T.FICE_CD <> S.FICE_CD
                    OR T.LOAD_CALC_APPLY <> S.LOAD_CALC_APPLY
                    OR T.FULLTIME_LIMIT_PCT <> S.FULLTIME_LIMIT_PCT
                    OR T.FULLTIM_LIMIT_WARN <> S.FULLTIM_LIMIT_WARN
                    OR T.PARTTIME_LIMIT_PCT <> S.PARTTIME_LIMIT_PCT
                    OR T.PARTTIM_LIMIT_WARN <> S.PARTTIM_LIMIT_WARN
                    OR T.ASSIGN_TYPE <> S.ASSIGN_TYPE
                    OR T.INSTRUCTOR_CLASS <> S.INSTRUCTOR_CLASS
                    OR T.CRSE_CNTCT_HRS_PCT <> S.CRSE_CNTCT_HRS_PCT
                    OR T.UNITS_ACAD_PRG_PCT <> S.UNITS_ACAD_PRG_PCT
                    OR T.LMS_FILE_TYPE <> S.LMS_FILE_TYPE
                    OR T.PHONE_TYPE <> S.PHONE_TYPE
                    OR T.ADDR_USAGE <> S.ADDR_USAGE
                    OR T.REPEAT_ENRL_CTL <> S.REPEAT_ENRL_CTL
                    OR T.REPEAT_ENRL_SUSP <> S.REPEAT_ENRL_SUSP
                    OR T.REPEAT_GRD_CK <> S.REPEAT_GRD_CK
                    OR T.REPEAT_GRD_SUSP <> S.REPEAT_GRD_SUSP
                    OR T.GRAD_NAME_CHG <> S.GRAD_NAME_CHG
                    OR T.PRINT_NID <> S.PRINT_NID
                    OR T.REPEAT_CHK_TOPIC <> S.REPEAT_CHK_TOPIC
                    OR T.SCC_AUS_DEST <> S.SCC_AUS_DEST
                    OR T.SCC_CAN_GOV_RPT <> S.SCC_CAN_GOV_RPT
                    OR T.SCC_NZL_ENR <> S.SCC_NZL_ENR
                    OR T.SCC_NZL_NZQA <> S.SCC_NZL_NZQA
                    OR T.SSR_USE_WEEKS <> S.SSR_USE_WEEKS
                    OR T.SSR_ENBL_ACAD_PROG <> S.SSR_ENBL_ACAD_PROG
                    OR T.SSR_CLASS_CANC_ENR <> S.SSR_CLASS_CANC_ENR
                    OR T.SSR_CLASS_CANC_NON <> S.SSR_CLASS_CANC_NON
                    OR T.EXT_USERID_OPT <> S.EXT_USERID_OPT
                    OR T.LMS_PROVIDER <> S.LMS_PROVIDER
                    OR T.E_ADDR_TYPE <> S.E_ADDR_TYPE
                    OR T.SCC_HE_USED_NLD <> S.SCC_HE_USED_NLD
                    OR T.DATA_ORIGIN = 'D'
   WHEN NOT MATCHED
   THEN
      INSERT     (T.INSTITUTION,
                  T.EFFDT,
                  T.SRC_SYS_ID,
                  T.EFF_STATUS,
                  T.DESCR,
                  T.DESCRSHORT,
                  T.DESCRFORMAL,
                  T.COUNTRY,
                  T.ADDRESS1,
                  T.ADDRESS2,
                  T.ADDRESS3,
                  T.ADDRESS4,
                  T.CITY,
                  T.NUM1,
                  T.NUM2,
                  T.HOUSE_TYPE,
                  T.ADDR_FIELD1,
                  T.ADDR_FIELD2,
                  T.ADDR_FIELD3,
                  T.COUNTY,
                  T.STATE,
                  T.POSTAL,
                  T.GEO_CODE,
                  T.IN_CITY_LIMIT,
                  T.GRADING_SCHEME,
                  T.GRADING_BASIS,
                  T.GRADING_BASIS_SCH,
                  T.CAMPUS,
                  T.STDNT_SPEC_PERM,
                  T.AUTO_ENRL_WAITLIST,
                  T.RESIDENCY_REQ,
                  T.FA_WDCAN_RSN,
                  T.ENRL_ACTION_REASON,
                  T.FACILITY_CONFLICT,
                  T.NSLC_AGD_RULE,
                  T.NSLC_MONTH_FACTOR,
                  T.STDNT_ATTR_COHORT,
                  T.CLASS_MTG_ATND_TYP,
                  T.FICE_CD,
                  T.LOAD_CALC_APPLY,
                  T.FULLTIME_LIMIT_PCT,
                  T.FULLTIM_LIMIT_WARN,
                  T.PARTTIME_LIMIT_PCT,
                  T.PARTTIM_LIMIT_WARN,
                  T.ASSIGN_TYPE,
                  T.INSTRUCTOR_CLASS,
                  T.CRSE_CNTCT_HRS_PCT,
                  T.UNITS_ACAD_PRG_PCT,
                  T.LMS_FILE_TYPE,
                  T.PHONE_TYPE,
                  T.ADDR_USAGE,
                  T.REPEAT_ENRL_CTL,
                  T.REPEAT_ENRL_SUSP,
                  T.REPEAT_GRD_CK,
                  T.REPEAT_GRD_SUSP,
                  T.GRAD_NAME_CHG,
                  T.PRINT_NID,
                  T.REPEAT_CHK_TOPIC,
                  T.SCC_AUS_DEST,
                  T.SCC_CAN_GOV_RPT,
                  T.SCC_NZL_ENR,
                  T.SCC_NZL_NZQA,
                  T.SSR_USE_WEEKS,
                  T.SSR_ENBL_ACAD_PROG,
                  T.SSR_CLASS_CANC_ENR,
                  T.SSR_CLASS_CANC_NON,
                  T.EXT_USERID_OPT,
                  T.LMS_PROVIDER,
                  T.E_ADDR_TYPE,
                  T.SCC_HE_USED_NLD,
                  T.LOAD_ERROR,
                  T.DATA_ORIGIN,
                  T.CREATED_EW_DTTM,
                  T.LASTUPD_EW_DTTM,
                  T.BATCH_SID)
          VALUES (S.INSTITUTION,
                  S.EFFDT,
                  'CS90',
                  S.EFF_STATUS,
                  S.DESCR,
                  S.DESCRSHORT,
                  S.DESCRFORMAL,
                  S.COUNTRY,
                  S.ADDRESS1,
                  S.ADDRESS2,
                  S.ADDRESS3,
                  S.ADDRESS4,
                  S.CITY,
                  S.NUM1,
                  S.NUM2,
                  S.HOUSE_TYPE,
                  S.ADDR_FIELD1,
                  S.ADDR_FIELD2,
                  S.ADDR_FIELD3,
                  S.COUNTY,
                  S.STATE,
                  S.POSTAL,
                  S.GEO_CODE,
                  S.IN_CITY_LIMIT,
                  S.GRADING_SCHEME,
                  S.GRADING_BASIS,
                  S.GRADING_BASIS_SCH,
                  S.CAMPUS,
                  S.STDNT_SPEC_PERM,
                  S.AUTO_ENRL_WAITLIST,
                  S.RESIDENCY_REQ,
                  S.FA_WDCAN_RSN,
                  S.ENRL_ACTION_REASON,
                  S.FACILITY_CONFLICT,
                  S.NSLC_AGD_RULE,
                  S.NSLC_MONTH_FACTOR,
                  S.STDNT_ATTR_COHORT,
                  S.CLASS_MTG_ATND_TYP,
                  S.FICE_CD,
                  S.LOAD_CALC_APPLY,
                  S.FULLTIME_LIMIT_PCT,
                  S.FULLTIM_LIMIT_WARN,
                  S.PARTTIME_LIMIT_PCT,
                  S.PARTTIM_LIMIT_WARN,
                  S.ASSIGN_TYPE,
                  S.INSTRUCTOR_CLASS,
                  S.CRSE_CNTCT_HRS_PCT,
                  S.UNITS_ACAD_PRG_PCT,
                  S.LMS_FILE_TYPE,
                  S.PHONE_TYPE,
                  S.ADDR_USAGE,
                  S.REPEAT_ENRL_CTL,
                  S.REPEAT_ENRL_SUSP,
                  S.REPEAT_GRD_CK,
                  S.REPEAT_GRD_SUSP,
                  S.GRAD_NAME_CHG,
                  S.PRINT_NID,
                  S.REPEAT_CHK_TOPIC,
                  S.SCC_AUS_DEST,
                  S.SCC_CAN_GOV_RPT,
                  S.SCC_NZL_ENR,
                  S.SCC_NZL_NZQA,
                  S.SSR_USE_WEEKS,
                  S.SSR_ENBL_ACAD_PROG,
                  S.SSR_CLASS_CANC_ENR,
                  S.SSR_CLASS_CANC_NON,
                  S.EXT_USERID_OPT,
                  S.LMS_PROVIDER,
                  S.E_ADDR_TYPE,
                  S.SCC_HE_USED_NLD,
                  'N',
                  'S',
                  SYSDATE,
                  SYSDATE,
                  1234);


   strSqlCommand := 'SET intRowCount';
   intRowCount := SQL%ROWCOUNT;

   strSqlCommand := 'commit';
   COMMIT;

   strMessage01 :=
         '# of PS_INSTITUTION_TBL rows merged: '
      || TO_CHAR (intRowCount, '999,999,999,999');
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
   COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL (
      i_TargetTableName   => 'PS_INSTITUTION_TBL',
      i_Action            => 'MERGE',
      i_RowCount          => intRowCount);


   strMessage01 := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'update TABLE_STATUS on AMSTG_OWNER.UM_STAGE_JOBS';

   UPDATE AMSTG_OWNER.UM_STAGE_JOBS
      SET TABLE_STATUS = 'Deleting', OLD_MAX_SCN = NEW_MAX_SCN
    WHERE TABLE_NAME = 'PS_INSTITUTION_TBL';

   strSqlCommand := 'commit';
   COMMIT;


   strMessage01 := 'Updating DATA_ORIGIN on AMSTG_OWNER.PS_INSTITUTION_TBL';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'update DATA_ORIGIN on AMSTG_OWNER.PS_INSTITUTION_TBL';

   UPDATE AMSTG_OWNER.PS_INSTITUTION_TBL T
      SET T.DATA_ORIGIN = 'D', T.LASTUPD_EW_DTTM = SYSDATE
    WHERE     T.DATA_ORIGIN <> 'D'
          AND EXISTS
                 (SELECT 1
                    FROM (SELECT INSTITUTION, EFFDT
                            FROM AMSTG_OWNER.PS_INSTITUTION_TBL T2
                           WHERE (SELECT DELETE_FLG
                                    FROM AMSTG_OWNER.UM_STAGE_JOBS
                                   WHERE TABLE_NAME = 'PS_INSTITUTION_TBL') =
                                    'Y'
                          MINUS
                          SELECT INSTITUTION, EFFDT
                            FROM SYSADM.PS_INSTITUTION_TBL@AMSOURCE
                           WHERE (SELECT DELETE_FLG
                                    FROM AMSTG_OWNER.UM_STAGE_JOBS
                                   WHERE TABLE_NAME = 'PS_INSTITUTION_TBL') =
                                    'Y'-- AND EMPLID <>'00386824'
                         ) S
                   WHERE     T.INSTITUTION = S.INSTITUTION
                         AND T.EFFDT = S.EFFDT
                         AND T.SRC_SYS_ID = 'CS90');

   strSqlCommand := 'SET intRowCount';
   intRowCount := SQL%ROWCOUNT;

   strSqlCommand := 'commit';
   COMMIT;

   strMessage01 :=
         '# of PS_INSTITUTION_TBL rows updated: '
      || TO_CHAR (intRowCount, '999,999,999,999');
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
   COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL (
      i_TargetTableName   => 'PS_INSTITUTION_TBL',
      i_Action            => 'UPDATE',
      i_RowCount          => intRowCount);


   strMessage01 := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'update END_DT on AMSTG_OWNER.UM_STAGE_JOBS';

   UPDATE AMSTG_OWNER.UM_STAGE_JOBS
      SET TABLE_STATUS = 'Complete', END_DT = SYSDATE
    WHERE TABLE_NAME = 'PS_INSTITUTION_TBL';

   strSqlCommand := 'commit';
   COMMIT;


   strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_SUCCESS';
   COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_SUCCESS;

   strMessage01 := strProcessName || ' is complete.';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);
EXCEPTION
   WHEN OTHERS
   THEN
      COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_EXCEPTION (
         i_SqlCommand   => strSqlCommand,
         i_SqlCode      => SQLCODE,
         i_SqlErrm      => SQLERRM);
END AM_PS_INSTITUTION_TBL_P;
/
