CREATE OR REPLACE PROCEDURE             "UM_F_FA_STDNT_AID_COMM_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
--George Adams
--Loads table                -- UM_F_FA_STDNT_AID_COMM
--V01 12/12/2018             -- srikanth ,pabbu converted to proc from sql scripts

------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_F_FA_STDNT_AID_COMM';
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

strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.UM_F_FA_STDNT_AID_COMM';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','UM_F_FA_STDNT_AID_COMM');

strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_FA_STDNT_AID_COMM disable constraint PK_UM_F_FA_STDNT_AID_COMM';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );
				
strMessage01    := 'Truncating table CSMRT_OWNER.UM_F_FA_STDNT_AID_COMM';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.UM_F_FA_STDNT_AID_COMM';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Inserting data into CSMRT_OWNER.UM_F_FA_STDNT_AID_COMM';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.UM_F_FA_STDNT_AID_COMM';				
insert /*+ append */ into UM_F_FA_STDNT_AID_COMM
   WITH   COMM
        AS (SELECT  /*+ INLINE PARALLEL(8) */ F.PERSON_SID,
                   V.AID_YEAR,
                   F.SEQ_3C,
                   F.SRC_SYS_ID,
                   F.COMMON_ID PERSON_ID,
                   F.SA_ID_TYPE,
                   F.COMM_DTTM,
                   F.INSTITUTION INSTITUTION_CD,
                   F.INSTITUTION_SID,
                   F.ADMIN_FUNC_SID,
                   F.COMM_CATEGORY,
                   F.COMM_CATEGORY_SD,
                   F.COMM_CATEGORY_LD,
                   F.COMM_CONTEXT,
                   F.COMM_CONTEXT_SD,
                   F.COMM_CONTEXT_LD,
                   F.COMM_METHOD,
                   F.DEPT_FUNC_SID,
                   F.COMM_DT,
                   F.PERSON_ASSIGNED_SID,
                   F.PERSON_COMPLETED_SID,
                   F.COMPLETED_COMM_FLG,
                   F.COMPLETED_DT,
                   F.UNSUCCESSFUL_FLG,
                   F.OUTCOME_REASON,
                   F.SCC_LETTER_CD,
                   F.SCC_LETTER_SD,
                   F.SCC_LETTER_LD,
                   F.LETTER_PRINTED_DT,
                   F.CHECKLIST_SEQ_3C,
                   F.CHECKLIST_SEQ,
                   F.COMMENT_PRINT_FLAG,
                   F.ORG_CONTACT,
                   F.PROCESS_INSTANCE,
                   F.VAR_DATA_SEQ,
                   F.VAR_DATA_SID,
                   F.JOINT_COMM_FLG,
                   F.SCC_COMM_LANG,
                   F.SCC_COMM_MTHD,
                   F.SCC_COMM_PROC,
                   F.COMM_COMMENTS,
                   DENSE_RANK ()
                   OVER (PARTITION BY F.PERSON_SID, V.AID_YEAR, F.SRC_SYS_ID
                         ORDER BY
                            F.PERSON_SID,
                            V.AID_YEAR,
                            F.SRC_SYS_ID,
                            F.SEQ_3C DESC)
                      COMM_ORDER
              FROM UM_F_COMM_PERSON F
                   JOIN PS_D_ADMIN_FUNC D
                      ON     F.ADMIN_FUNC_SID = D.ADMIN_FUNC_SID
                         AND D.ADMIN_FUNCTION IN ('FINA', 'GEN', 'ADMP')
                   JOIN
                   (SELECT  /*+ INLINE PARALLEL(8) */ DISTINCT COMMON_ID, AID_YEAR, VAR_DATA_SID
                      FROM PS_D_VAR_DATA
                     WHERE     ADMIN_FUNCTION IN ('FINA', '-')
                           AND DATA_ORIGIN <> 'D'
                    UNION
                    SELECT  /*+ INLINE PARALLEL(8) */ DISTINCT COMMON_ID, AID_YEAR, VAR_DATA_SID
                      FROM PS_D_VAR_DATA
                      WHERE ADMIN_FUNCTION = 'ADMP'
                           AND DATA_ORIGIN <> 'D'  
                           AND COMMON_ID || ADM_APPL_NBR IN (SELECT DISTINCT PERSON_ID||ADM_APPL_NBR FROM UM_F_ADM_APPL_STAT)) V
                      ON F.VAR_DATA_SID = V.VAR_DATA_SID)
   SELECT  /*+ INLINE PARALLEL(8) */ COMM.INSTITUTION_CD,
          COMM.PERSON_ID,
          NVL (COMM.AID_YEAR, '-') AID_YEAR,
          COMM.SEQ_3C,
          COMM.SRC_SYS_ID,
          COMM.INSTITUTION_SID,
          COMM.PERSON_SID,
          COMM.SA_ID_TYPE,
          COMM.COMM_DTTM,
          NVL (COMM.ADMIN_FUNC_SID, 2147483646) ADMIN_FUNC_SID,
          COMM.COMM_CATEGORY,
          COMM.COMM_CATEGORY_SD,
          COMM.COMM_CATEGORY_LD,
          COMM.COMM_CONTEXT,
          COMM.COMM_CONTEXT_SD,
          COMM.COMM_CONTEXT_LD,
          COMM.COMM_METHOD,
          NVL (
             (SELECT  /*+ INLINE PARALLEL(8) */ MIN (X.XLATSHORTNAME)
                FROM UM_D_XLATITEM_VW X
               WHERE     X.FIELDNAME = 'COMM_METHOD'
                     AND X.FIELDVALUE = COMM.COMM_METHOD),
             '-')
             COMM_METHOD_SD,
          NVL (
             (SELECT  /*+ INLINE PARALLEL(8) */ MIN (X.XLATLONGNAME)
                FROM UM_D_XLATITEM_VW X
               WHERE     X.FIELDNAME = 'COMM_METHOD'
                     AND X.FIELDVALUE = COMM.COMM_METHOD),
             '-')
             COMM_METHOD_LD,
          NVL (COMM.DEPT_FUNC_SID, 2147483646) DEPT_FUNC_SID,
          COMM.COMM_DT,
          NVL (COMM.PERSON_ASSIGNED_SID, 2147483646) PERSON_ASSIGNED_SID,
          NVL (COMM.PERSON_COMPLETED_SID, 2147483646) PERSON_COMPLETED_SID,
          COMM.COMPLETED_COMM_FLG,
          COMM.COMPLETED_DT,
          COMM.UNSUCCESSFUL_FLG,
          COMM.OUTCOME_REASON,
          NVL (
             (SELECT  /*+ INLINE PARALLEL(8) */ MIN (X.XLATSHORTNAME)
                FROM UM_D_XLATITEM_VW X
               WHERE     X.FIELDNAME = 'OUTCOME_REASON'
                     AND X.FIELDVALUE = COMM.OUTCOME_REASON),
             '-')
             OUTCOME_REASON_SD,
          NVL (
             (SELECT  /*+ INLINE PARALLEL(8) */ MIN (X.XLATLONGNAME)
                FROM UM_D_XLATITEM_VW X
               WHERE     X.FIELDNAME = 'OUTCOME_REASON'
                     AND X.FIELDVALUE = COMM.OUTCOME_REASON),
             '-')
             OUTCOME_REASON_LD,
          COMM.SCC_LETTER_CD,
          COMM.SCC_LETTER_SD,
          COMM.SCC_LETTER_LD,
          COMM.LETTER_PRINTED_DT,
          COMM.CHECKLIST_SEQ_3C,
          COMM.CHECKLIST_SEQ,
          COMM.COMMENT_PRINT_FLAG,
          COMM.ORG_CONTACT,
          COMM.PROCESS_INSTANCE,
          COMM.VAR_DATA_SEQ,
          COMM.VAR_DATA_SID,
          COMM.JOINT_COMM_FLG,
          COMM.SCC_COMM_LANG,
          COMM.SCC_COMM_MTHD,
          COMM.SCC_COMM_PROC,
          COMM.COMM_COMMENTS,
          COMM.COMM_ORDER
     FROM COMM;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_F_FA_STDNT_AID_COMM rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_FA_STDNT_AID_COMM',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.UM_F_FA_STDNT_AID_COMM';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_FA_STDNT_AID_COMM enable constraint PK_UM_F_FA_STDNT_AID_COMM';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );
				
COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','UM_F_FA_STDNT_AID_COMM');

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

END UM_F_FA_STDNT_AID_COMM_P;
/
