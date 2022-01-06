CREATE OR REPLACE PROCEDURE             "UM_F_FA_STDNT_AID_CHK_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
--George Adams
--Loads table                -- UM_F_FA_STDNT_AID_CHK
--V01 12/12/2018             -- srikanth ,pabbu converted to proc from sql scripts

------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_F_FA_STDNT_AID_CHK';
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

strMessage01    := 'Truncating table CSMRT_OWNER.UM_F_FA_STDNT_AID_CHK';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.UM_F_FA_STDNT_AID_CHK';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.UM_F_FA_STDNT_AID_CHK';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','UM_F_FA_STDNT_AID_CHK');

strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_FA_STDNT_AID_CHK disable constraint PK_UM_F_FA_STDNT_AID_CHK';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Inserting data into CSMRT_OWNER.UM_F_FA_STDNT_AID_CHK';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.UM_F_FA_STDNT_AID_CHK';
insert /*+ append parallel(8) enable_parallel_dml */ into UM_F_FA_STDNT_AID_CHK
   WITH CHK
        AS (SELECT  /*+ INLINE PARALLEL(8) */ F.PERSON_SID,
                   V.AID_YEAR,
                   F.COMMON_ID,
                   F.SEQ_3C,
                   R.CHECKLIST_SEQ,
--                   F.CHKLIST_DT_SID,
                   F.CHKLIST_DT,
                   F.CHKLIST_TM,
                   F.ADMIN_FUNC_SID,
                   F.INSTITUTION_SID,
                   F.SRC_SYS_ID,
                   F.CHKLIST_CD_SID,
                   F.CHKLIST_STAT_SID,
--                   F.STATUS_DT_SID,
                   F.STATUS_DT,
                   F.STAT_CHG_SID,
--                   F.DUE_DT_SID,
                   F.DUE_DT,
                   F.DEPT_SID,
                   F.RESPONSIBLE_SID,
                   F.VAR_DATA_SID,
                   F.TRACKING_SEQ,
                   F.DUE_AMT,
                   F.ADMIN_FUNC_AREA,
                   V.ADM_APPL_NBR,          -- Added
                   CD.CHECKLIST_CD,         -- Added
                   S.CHKLIST_STAT_ID,       -- Added
                   R.ITEM_CD_SID,
                   C.CHKLST_ITEM_CD,
                   R.ITEM_STATUS,
                   R.ITEM_STATUS_SD,
                   R.ITEM_STATUS_LD,
                   R.STATUS_DT ITEM_STATUS_DT,
                   R.STATUS_CHANGE_ID ITEM_STATUS_CHANGE_ID,
                   R.DUE_DT ITEM_DUE_DT,
                   R.DUE_AMT ITEM_DUE_AMT,
                   R.RESPONSIBLE_ID ITEM_RESPONSIBLE_ID,
                   R.ASSOC_ID,
                   R.NAME,
                   R.COMM_KEY,
                   DENSE_RANK ()
                   OVER (PARTITION BY F.PERSON_SID, V.AID_YEAR, F.SRC_SYS_ID
                         ORDER BY
                            F.PERSON_SID,
                            V.AID_YEAR,
                            F.SRC_SYS_ID,
                            F.SEQ_3C DESC)
                      CHKLIST_ORDER,
                      F.COMM_COMMENTS
              FROM PS_F_CHKLST_PERSON F
                   JOIN PS_R_CHKLST_ITEM R
                     ON F.COMMON_ID = R.COMMON_ID
                    AND F.SEQ_3C = R.SEQ_3C
                    AND F.SRC_SYS_ID = R.SRC_SYS_ID
                    AND F.DATA_ORIGIN <> 'D'
                    AND R.DATA_ORIGIN <> 'D'
                   join PS_D_CHKLST_CD CD
                     on F.CHKLIST_CD_SID = CD.CHKLIST_CD_SID
                   join PS_D_CHKLST_STAT S
                     on F.CHKLIST_STAT_SID = S.CHKLIST_STAT_SID
                   JOIN PS_D_ITEM_CD C
                     ON R.ITEM_CD_SID = C.ITEM_CD_SID
                   JOIN PS_D_ADMIN_FUNC D
                     ON F.ADMIN_FUNC_SID = D.ADMIN_FUNC_SID
                    AND D.ADMIN_FUNCTION IN ('FINA', 'GEN', 'ADMP')
                   JOIN
                   (SELECT  /*+ INLINE PARALLEL(8) */ DISTINCT COMMON_ID, AID_YEAR, ADM_APPL_NBR, VAR_DATA_SID
                      FROM PS_D_VAR_DATA
                      WHERE ADMIN_FUNCTION IN ('FINA', '-')
                           AND DATA_ORIGIN <> 'D'
                   UNION
                   SELECT  /*+ INLINE PARALLEL(8) */ DISTINCT COMMON_ID, AID_YEAR, ADM_APPL_NBR, VAR_DATA_SID
                      FROM PS_D_VAR_DATA
                      WHERE ADMIN_FUNCTION = 'ADMP'
                           AND DATA_ORIGIN <> 'D'
                           AND COMMON_ID || ADM_APPL_NBR IN (SELECT DISTINCT PERSON_ID||ADM_APPL_NBR FROM UM_F_ADM_APPL_STAT)
                            ) V
                      ON F.VAR_DATA_SID = V.VAR_DATA_SID)
   SELECT  /*+ INLINE PARALLEL(8) */ INST.INSTITUTION_CD,
          PER.PERSON_ID,
          NVL (CHK.AID_YEAR, '-') AID_YEAR,
          CHK.SEQ_3C,
          CHK.CHECKLIST_SEQ,
          CHK.SRC_SYS_ID,
          CHK.INSTITUTION_SID,
          CHK.PERSON_SID,
          PER.PERSON_ID COMMON_ID,
--          TO_DATE (CHK.CHKLIST_DT_SID, 'YYYYMMDD') CHKLIST_DT,
          CHK.CHKLIST_DT,
          CHK.CHKLIST_TM,
          NVL (CHK.ADMIN_FUNC_SID, 2147483646) ADMIN_FUNC_SID,
          NVL (CHK.CHKLIST_CD_SID, 2147483646) CHKLIST_CD_SID,
          NVL (CHK.CHKLIST_STAT_SID, 2147483646) CHKLIST_STAT_SID,
--          TO_DATE (CHK.STATUS_DT_SID, 'YYYYMMDD') STATUS_DT,
          CHK.STATUS_DT,
          NVL (CHK.STAT_CHG_SID, 2147483646) STAT_CHG_SID,
--          TO_DATE (CHK.DUE_DT_SID, 'YYYYMMDD') DUE_DT,
          CHK.DUE_DT,
          NVL (CHK.DEPT_SID, 2147483646) DEPT_SID,
          NVL (CHK.RESPONSIBLE_SID, 2147483646) RESPONSIBLE_SID,
          NVL (CHK.VAR_DATA_SID, 2147483646) VAR_DATA_SID,
          CHK.TRACKING_SEQ,
          CHK.DUE_AMT,
          CHK.ADMIN_FUNC_AREA,
          CHK.ADM_APPL_NBR,         -- Added
          CHK.CHECKLIST_CD,         -- Added
          CHK.CHKLIST_STAT_ID,      -- Added
          CHK.ITEM_CD_SID,
          CHK.CHKLST_ITEM_CD,       -- Added
          CHK.ITEM_STATUS,
          CHK.ITEM_STATUS_SD,
          CHK.ITEM_STATUS_LD,
          CHK.ITEM_STATUS_DT,
          CHK.ITEM_STATUS_CHANGE_ID,
          CHK.ITEM_DUE_DT,
          CHK.DUE_AMT ITEM_DUE_AMT,
          CHK.ITEM_RESPONSIBLE_ID,
          CHK.ASSOC_ID,
          CHK.NAME,
          CHK.COMM_KEY,
          CHK.CHKLIST_ORDER,
       ROW_NUMBER () OVER (PARTITION BY INSTITUTION_CD, PERSON_ID, AID_YEAR, CHKLIST_CD_SID
                               ORDER BY STATUS_DT DESC, SEQ_3C DESC, CHECKLIST_SEQ DESC) CHKLIST_CD_ORDER,
       ROW_NUMBER () OVER (PARTITION BY INSTITUTION_CD, PERSON_ID, AID_YEAR, SEQ_3C, ITEM_CD_SID
                               ORDER BY ITEM_STATUS_DT DESC, CHECKLIST_SEQ DESC) CHKLIST_ITEM_ORDER,
       'S' DATA_ORIGIN,
       SYSDATE CREATED_EW_DTTM,
       SYSDATE LASTUPD_EW_DTTM,
       CHK.COMM_COMMENTS        -- Dec 2019
     FROM CHK CHK,
          PS_D_INSTITUTION INST,
          PS_D_PERSON PER
     WHERE CHK.PERSON_SID = PER.PERSON_SID
       AND CHK.INSTITUTION_SID = INST.INSTITUTION_SID
       and CHK.PERSON_SID <> 2147483646     -- Jan 2019
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_F_FA_STDNT_AID_CHK rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_FA_STDNT_AID_CHK',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.UM_F_FA_STDNT_AID_CHK';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_FA_STDNT_AID_CHK enable constraint PK_UM_F_FA_STDNT_AID_CHK';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','UM_F_FA_STDNT_AID_CHK');

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

END UM_F_FA_STDNT_AID_CHK_P;
/
