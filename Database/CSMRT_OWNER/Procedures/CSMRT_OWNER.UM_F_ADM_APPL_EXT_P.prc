DROP PROCEDURE CSMRT_OWNER.UM_F_ADM_APPL_EXT_P
/

--
-- UM_F_ADM_APPL_EXT_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER.UM_F_ADM_APPL_EXT_P AUTHID CURRENT_USER IS

------------------------------------------------------------------------
--George Adams
--Loads table                -- UM_F_ADM_APPL_EXT
--V01 12/12/2018             -- srikanth ,pabbu converted to proc from sql scripts
--V01.1 08/21/2019, SMT-8335   -- Doucette ,James added UM_OVRD_GPA_FLG
--                             -- Added schema qualifiers to table references.
--V01.2 09/10/2019  SMT-8300 09/06/2017,    James Doucette
--                                          Added two new fields

------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_F_ADM_APPL_EXT';
        intProcessSid                   Integer;
        dtProcessStart                  Date            := SYSDATE;
        strMessage01                    Varchar2(4000);
        strMessage02                    Varchar2(512);
        strMessage03                    Varchar2(512)   :='';
        strNewLine                      Varchar2(2)     := chr(13) || chr(10);
        strSqlCommand                   Varchar2(32767) :='';
        strSqlDynamic                   Varchar2(32767) :='';
        strClientInfo                   Varchar2(100);
        intRowCount                     Integer;
        intTotalRowCount                Integer         := 0;
        numSqlCode                      Number;
        strSqlErrm                      Varchar2(4000);
        intTries                        Integer;

BEGIN

strSqlCommand := 'DBMS_APPLICATION_INFO.SET_CLIENT_INFO';
DBMS_APPLICATION_INFO.SET_CLIENT_INFO (strProcessName);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_INIT';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_INIT
        (
                i_MartId                => strMartId,
                i_ProcessName           => strProcessName,
                i_ProcessStartTime      => dtProcessStart,
                o_ProcessSid            => intProcessSid
        );

strMessage01    := 'Truncating table CSMRT_OWNER.UM_F_ADM_APPL_EXT';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.UM_F_ADM_APPL_EXT';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.UM_F_ADM_APPL_EXT';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','UM_F_ADM_APPL_EXT');

--strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_ADM_APPL_EXT disable constraint PK_UM_F_ADM_APPL_EXT';
--strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
--COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
--                (
--                i_SqlStatement          => strSqlDynamic,
--                i_MaxTries              => 10,
--                i_WaitSeconds           => 10,
--                o_Tries                 => intTries
--                );

strMessage01    := 'Inserting data into CSMRT_OWNER.UM_F_ADM_APPL_EXT';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.UM_F_ADM_APPL_EXT';
insert /*+ append enable_parallel_dml parallel(8) */ into CSMRT_OWNER.UM_F_ADM_APPL_EXT
   WITH A
        AS (SELECT /*+ INLINE PARALLEL(8) */
            DISTINCT APPLCNT_SID,
                     INSTITUTION_SID,
                     SRC_SYS_ID
              FROM CSMRT_OWNER.UM_F_ADM_APPL_STAT)
   SELECT /*+ INLINE PARALLEL(8) */
          A.APPLCNT_SID,
          A.INSTITUTION_SID,
          NVL (E.EXT_ORG_SID, 2147483646) EXT_ORG_SID,
          NVL (E.EXT_ACAD_CAR_SID, 2147483646) EXT_ACAD_CAR_SID,
          NVL (E.EXT_DATA_NBR, 0) EXT_DATA_NBR,
          NVL (E.EXT_SUMM_TYPE_SID, 2147483646) EXT_SUMM_TYPE_SID,
          A.SRC_SYS_ID,
          NVL (E.PERSON_ID,'-') PERSON_ID,                      -- Move to UM_F_ADM_APPL_STAT!!!
          NVL (E.INSTITUTION_CD,'-') INSTITUTION_CD,            -- Move to UM_F_ADM_APPL_STAT!!!
          NVL (E.EXT_ORG_ID,'-') EXT_ORG_ID,                    -- Move to UM_F_ADM_APPL_STAT!!!
          NVL (E.EXT_ACAD_CAR_ID,'-') EXT_ACAD_CAR_ID,          -- Move to UM_F_ADM_APPL_STAT!!!
          NVL (E.EXT_SUMM_TYPE_ID,'-') EXT_SUMM_TYPE_ID,        -- Move to UM_F_ADM_APPL_STAT!!!
          NVL (E.EXT_ACAD_LVL_SID, 2147483646) EXT_ACAD_LVL_SID,
          NVL (E.EXT_TERM_YEAR_SID, 2147483646) EXT_TERM_YEAR_SID,
          NVL (E.EXT_TERM_SID, 2147483646) EXT_TERM_SID,
          NVL (E.ACAD_UNIT_TYPE_SID, 2147483646) ACAD_UNIT_TYPE_SID,
          NVL (E.ACAD_RANK_TYPE_SID, 2147483646) ACAD_RANK_TYPE_SID,
          NVL (E.GPA_TYPE_SID, 2147483646) GPA_TYPE_SID,
          E.LS_DATA_SOURCE LS_DATA_SOURCE,
          NVL ((SELECT MIN (X.XLATSHORTNAME)
                  FROM CSMRT_OWNER.UM_D_XLATITEM_VW X
                 WHERE X.FIELDNAME = 'LS_DATA_SOURCE'
                   AND X.FIELDVALUE = LS_DATA_SOURCE), ' ') LS_DATA_SOURCE_SD,
          NVL ((SELECT MIN (X.XLATLONGNAME)
                  FROM CSMRT_OWNER.UM_D_XLATITEM_VW X
                 WHERE X.FIELDNAME = 'LS_DATA_SOURCE'
                   AND X.FIELDVALUE = LS_DATA_SOURCE), ' ') LS_DATA_SOURCE_LD,
          E.TRNSCR_FLG TRNSCR_FLG,
          NVL ((SELECT MIN (X.XLATLONGNAME)
                  FROM CSMRT_OWNER.UM_D_XLATITEM_VW X
                 WHERE X.FIELDNAME = 'TRANSCRIPT_FLAG'
                   AND X.FIELDVALUE = TRNSCR_FLG), ' ') TRNSCR_FLG_LD,
          E.TRNSCR_TYPE TRNSCR_TYPE,
          NVL ((SELECT MIN (X.XLATLONGNAME)
                  FROM CSMRT_OWNER.UM_D_XLATITEM_VW X
                 WHERE X.FIELDNAME = 'TRANSCRIPT_TYPE'
                   AND X.FIELDVALUE = TRNSCR_TYPE), ' ') TRNSCR_TYPE_LD,
          E.TRNSCR_STATUS TRNSCR_STATUS,
          (case when E.TRNSCR_DT = '01-JAN-1900' then NULL else E.TRNSCR_DT end) TRNSCR_DT,
          (case when E.FROM_DT = '01-JAN-1900' then NULL else E.FROM_DT end) FROM_DT,
          (case when E.TO_DT = '01-JAN-1900' then NULL else E.TO_DT end) TO_DT,
          NVL (E.D_EXT_ACAD_LVL_SID, 2147483646) D_EXT_ACAD_LVL_SID,
          NVL (E.D_EXT_TERM_YEAR_SID, 2147483646) D_EXT_TERM_YEAR_SID,
          NVL (E.D_EXT_TERM_SID, 2147483646) D_EXT_TERM_SID,
          (CASE WHEN E.UNITS_ATTMPTD = 0 THEN NULL
                ELSE E.UNITS_ATTMPTD
                END) UNITS_ATTMPTD,
          (CASE WHEN E.UNITS_CMPLTD = 0 THEN NULL
                ELSE E.UNITS_CMPLTD
                END) UNITS_CMPLTD,
          (CASE WHEN E.CLASS_RANK = 0 THEN NULL
                ELSE E.CLASS_RANK
                END) CLASS_RANK,
          (CASE WHEN E.CLASS_SIZE = 0 THEN NULL
                ELSE E.CLASS_SIZE
                END) CLASS_SIZE,
          (CASE WHEN E.CLASS_PERCENTILE = 0 THEN NULL
                ELSE E.CLASS_PERCENTILE
                END) CLASS_PERCENTILE,
          (CASE WHEN E.EXT_GPA = 0 THEN NULL
                ELSE E.EXT_GPA
                END) EXT_GPA,
          (CASE WHEN E.CONVERTED_GPA = 0 THEN NULL
                ELSE E.CONVERTED_GPA
                END) CONVERTED_GPA,
          (CASE WHEN E.UM_CUM_CREDIT = 0 THEN NULL
                ELSE E.UM_CUM_CREDIT
                END) UM_CUM_CREDIT,
          (CASE WHEN E.UM_CUM_GPA = 0 THEN NULL
                ELSE E.UM_CUM_GPA
                END) UM_CUM_GPA,
          (CASE WHEN E.UM_CUM_QP = 0 THEN NULL
                ELSE E.UM_CUM_QP
                END) UM_CUM_QP,
          (CASE WHEN E.UM_CUM_CREDIT_AGG = 0 THEN NULL      -- Aug 2022
                ELSE E.UM_CUM_CREDIT_AGG
                END) UM_CUM_CREDIT_AGG,
          (CASE WHEN E.UM_CUM_GPA_AGG = 0 THEN NULL         -- Aug 2022
                ELSE E.UM_CUM_GPA_AGG
                END) UM_CUM_GPA_AGG,
          (CASE WHEN E.UM_CUM_QP_AGG = 0 THEN NULL          -- Aug 2022
                ELSE E.UM_CUM_QP_AGG
                END) UM_CUM_QP_AGG,
          E.UM_GPA_EXCLUDE_FLG,
          (CASE WHEN E.UM_EXT_ORG_CR = 0 THEN NULL
                ELSE E.UM_EXT_ORG_CR
                END) UM_EXT_ORG_CR,
          (CASE WHEN E.UM_EXT_ORG_QP = 0 THEN NULL
                ELSE E.UM_EXT_ORG_QP
                END) UM_EXT_ORG_QP,
          (CASE WHEN E.UM_EXT_ORG_GPA = 0 THEN NULL
                ELSE E.UM_EXT_ORG_GPA
                END) UM_EXT_ORG_GPA,
          (CASE WHEN E.UM_EXT_ORG_CNV_CR = 0 THEN NULL
                ELSE E.UM_EXT_ORG_CNV_CR
                END) UM_EXT_ORG_CNV_CR,
          (CASE WHEN E.UM_EXT_ORG_CNV_GPA = 0 THEN NULL
                ELSE E.UM_EXT_ORG_CNV_GPA
                END) UM_EXT_ORG_CNV_GPA,
          (CASE WHEN E.UM_EXT_ORG_CNV_QP = 0 THEN NULL
                ELSE E.UM_EXT_ORG_CNV_QP
                END) UM_EXT_ORG_CNV_QP,
          E.UM_1_OVRD_HSGPA_FLG,            -- Moved 8/30/2019
          E.UM_GPA_OVRD_FLG,                -- Moved 8/30/2019
		  (CASE WHEN UM_GPA_OVRD_FLG = 'Y' OR UM_1_OVRD_HSGPA_FLG = 'Y' THEN 'Y'
                ELSE 'N'
                END) UM_OVRD_GPA_FLG,       -- 8/20/2019, CSR 8335
          (CASE WHEN E.UM_CONVERT_GPA = 0 THEN NULL
                ELSE E.UM_CONVERT_GPA
                END) UM_CONVERT_GPA,
          ROW_NUMBER () OVER (PARTITION BY A.APPLCNT_SID, A.INSTITUTION_SID, NVL (E.EXT_ORG_SID, 2147483646),
                                NVL (E.EXT_ACAD_CAR_SID, 2147483646), NVL (E.EXT_SUMM_TYPE_SID, 2147483646),
                                A.SRC_SYS_ID
                                  ORDER BY NVL (E.EXT_DATA_NBR, 0) DESC) ADM_APPL_EXT_ORDER,
          BEST_SUMM_TYPE_GPA_FLG,
          max(case when EXT_SUMM_TYPE_ID IN ('HSOV', '-') and BEST_SUMM_TYPE_GPA_FLG = 'Y' then decode(CONVERTED_GPA,0,NULL,CONVERTED_GPA) else NULL end)
             over (partition by A.APPLCNT_SID, A.INSTITUTION_SID, A.SRC_SYS_ID) BEST_HS_GPA,
		  (CASE WHEN E.UM_EXT_OR_MTSC_GPA = 0 THEN NULL
                ELSE E.UM_EXT_OR_MTSC_GPA
                END)  UM_EXT_OR_MTSC_GPA,       -- SMT-8300
          (CASE WHEN E.MS_CONVERT_GPA = 0 THEN NULL
                ELSE E.MS_CONVERT_GPA
                END)  MS_CONVERT_GPA,           -- SMT-8300
          E.MAX_DATA_ROW,         -- Aug 2022
		  NVL (E.DATA_ORIGIN, 'S') DATA_ORIGIN,
		  NVL (E.CREATED_EW_DTTM, sysdate) CREATED_EW_DTTM,
		  NVL (E.LASTUPD_EW_DTTM, sysdate) LASTUPD_EW_DTTM
     FROM A
          LEFT OUTER JOIN CSMRT_OWNER.PS_F_EXT_ACAD_SUMM E
             ON A.APPLCNT_SID = E.PERSON_SID
            AND A.INSTITUTION_SID = E.INSTITUTION_SID
            AND A.SRC_SYS_ID = E.SRC_SYS_ID
            AND NVL (E.DATA_ORIGIN, '-') <> 'D'
    where A.APPLCNT_SID <> 2147483646         -- Aug 2018
      and A.INSTITUTION_SID <> 2147483646     -- Aug 2018
;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_F_ADM_APPL_EXT rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_ADM_APPL_EXT',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.UM_F_ADM_APPL_EXT';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

--strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_ADM_APPL_EXT enable constraint PK_UM_F_ADM_APPL_EXT';
--strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
--COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
--                (
--                i_SqlStatement          => strSqlDynamic,
--                i_MaxTries              => 10,
--                i_WaitSeconds           => 10,
--                o_Tries                 => intTries
--                );

COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','UM_F_ADM_APPL_EXT');

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_SUCCESS';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_SUCCESS;

strMessage01    := strProcessName || ' is complete.';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

EXCEPTION
    WHEN OTHERS THEN
        COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_EXCEPTION
                (
                        i_SqlCommand   => strSqlCommand,
                        i_SqlCode      => SQLCODE,
                        i_SqlErrm      => SQLERRM
                );

END UM_F_ADM_APPL_EXT_P;
/
