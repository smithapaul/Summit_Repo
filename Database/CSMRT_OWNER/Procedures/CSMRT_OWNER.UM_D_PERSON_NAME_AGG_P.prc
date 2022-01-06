CREATE OR REPLACE PROCEDURE             "UM_D_PERSON_NAME_AGG_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
--George Adams
--Loads table                -- UM_D_PERSON_NAME_AGG
--V01 04/25/2019             -- Doucette, James new procedure for updated table def.

------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_D_PERSON_NAME_AGG';
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

strMessage01    := 'Truncating table CSMRT_OWNER.UM_D_PERSON_NAME_AGG';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.UM_D_PERSON_NAME_AGG';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.UM_D_PERSON_NAME_AGG';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','UM_D_PERSON_NAME_AGG');

strSqlDynamic   := 'alter table CSMRT_OWNER.UM_D_PERSON_NAME_AGG disable constraint PK_UM_D_PERSON_NAME_AGG';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Inserting data into CSMRT_OWNER.UM_D_PERSON_NAME_AGG';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.UM_D_PERSON_NAME_AGG';
insert /*+ append parallel(8) enable_parallel_dml */
  into UM_D_PERSON_NAME_AGG
      SELECT /*+ parallel (8) */PERSON_ID,
             SRC_SYS_ID,
             PERSON_SID,
             MAX (DECODE (PRI_ORDER, 1, NAME, '-'))            PRI_NAME,
             MAX (DECODE (PRI_ORDER, 1, FIRST_NAME, '-'))      PRI_FIRST_NAME,
             MAX (DECODE (PRI_ORDER, 1, decode(trim(MIDDLE_NAME),'-','',MIDDLE_NAME), ''))     PRI_MIDDLE_NAME,
             MAX (DECODE (PRI_ORDER, 1, LAST_NAME, '-'))       PRI_LAST_NAME,
             MAX (DECODE (PRI_ORDER, 1, NAME_PREFIX, '-'))     PRI_PREFIX,
             MAX (DECODE (PRI_ORDER, 1, NAME_SUFFIX, '-'))     PRI_SUFFIX,
             MAX (DECODE (DEG_ORDER, 1, NAME, '-'))            DEG_NAME,
             MAX (DECODE (DEG_ORDER, 1, FIRST_NAME, '-'))      DEG_FIRST_NAME,
             MAX (DECODE (DEG_ORDER, 1, decode(trim(MIDDLE_NAME),'-','',MIDDLE_NAME), ''))     DEG_MIDDLE_NAME,
             MAX (DECODE (DEG_ORDER, 1, LAST_NAME, '-'))       DEG_LAST_NAME,
             MAX (DECODE (DEG_ORDER, 1, NAME_PREFIX, '-'))     DEG_PREFIX,
             MAX (DECODE (DEG_ORDER, 1, NAME_SUFFIX, '-'))     DEG_SUFFIX,
             MAX (DECODE (PRF_ORDER, 1, NAME, '-'))            PRF_NAME,
             MAX (DECODE (PRF_ORDER, 1, FIRST_NAME, '-'))      PRF_FIRST_NAME,
             MAX (DECODE (PRF_ORDER, 1, decode(trim(MIDDLE_NAME),'-','',MIDDLE_NAME), ''))     PRF_MIDDLE_NAME,
             MAX (DECODE (PRF_ORDER, 1, LAST_NAME, '-'))       PRF_LAST_NAME,
             MAX (DECODE (PRF_ORDER, 1, NAME_PREFIX, '-'))     PRF_PREFIX,
             MAX (DECODE (PRF_ORDER, 1, NAME_SUFFIX, '-'))     PRF_SUFFIX,
             MAX (DECODE (AKA_ORDER, 1, NAME, '-'))            AKA_NAME,
             MAX (DECODE (AKA_ORDER, 1, FIRST_NAME, '-'))      AKA_FIRST_NAME,
             MAX (DECODE (AKA_ORDER, 1, decode(trim(MIDDLE_NAME),'-','',MIDDLE_NAME), ''))     AKA_MIDDLE_NAME,
             MAX (DECODE (AKA_ORDER, 1, LAST_NAME, '-'))       AKA_LAST_NAME,
             MAX (DECODE (AKA_ORDER, 1, NAME_PREFIX, '-'))     AKA_PREFIX,
             MAX (DECODE (AKA_ORDER, 1, NAME_SUFFIX, '-'))     AKA_SUFFIX,
             MAX (DECODE (CPS_ORDER, 1, NAME, '-'))            CPS_NAME,
             MAX (DECODE (CPS_ORDER, 1, FIRST_NAME, '-'))      CPS_FIRST_NAME,
             MAX (DECODE (CPS_ORDER, 1, decode(trim(MIDDLE_NAME),'-','',MIDDLE_NAME), ''))     CPS_MIDDLE_NAME,
             MAX (DECODE (CPS_ORDER, 1, LAST_NAME, '-'))       CPS_LAST_NAME,
             MAX (DECODE (CPS_ORDER, 1, NAME_PREFIX, '-'))     CPS_PREFIX,
             MAX (DECODE (CPS_ORDER, 1, NAME_SUFFIX, '-'))     CPS_SUFFIX,
			 'S' DATA_ORIGIN, SYSDATE CREATED_EW_DTTM, SYSDATE LASTUPD_EW_DTTM
        FROM UM_D_PERSON_NAME
       where DATA_ORIGIN <> 'D'     -- Oct 2019
    GROUP BY PERSON_ID, SRC_SYS_ID, PERSON_SID;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_D_PERSON_NAME_AGG rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);


strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.UM_D_PERSON_NAME_AGG';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'alter table CSMRT_OWNER.UM_D_PERSON_NAME_AGG enable constraint PK_UM_D_PERSON_NAME_AGG';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','UM_D_PERSON_NAME_AGG');

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

END UM_D_PERSON_NAME_AGG_P;
/
