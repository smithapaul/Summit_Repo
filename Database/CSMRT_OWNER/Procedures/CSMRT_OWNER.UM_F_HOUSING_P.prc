CREATE OR REPLACE PROCEDURE             "UM_F_HOUSING_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
--George Adams
--Loads table          -- UM_F_HOUSING
--V 1.0 4/30/2020      -- J Doucette initial creation.

------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_F_HOUSING';
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

				
strMessage01    := 'Truncating table CSMRT_OWNER.UM_F_HOUSING';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.UM_F_HOUSING';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.UM_F_HOUSING';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','UM_F_HOUSING');

strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_HOUSING disable constraint PK_UM_F_HOUSING';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );
strMessage01    := 'Inserting data into CSMRT_OWNER.UM_F_HOUSING';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.UM_F_HOUSING';				
insert /*+ append */ into CSMRT_OWNER.UM_F_HOUSING 
  with Q1 as (
select INSTITUTION INSTITUTION_CD, EMPLID PERSON_ID, COMMENT1 ACAD_YEAR_LD, SRC_SYS_ID,
       APPLIES_AT_DATE APPLIED_DT, COMPLETED_DT, AGREEMENT_DATE AGREEMENT_DT,
       COMMENTS_256 HOUSING_TERM_LD, DATE_LOADED HOUSING_LOAD_DT
  from CSSTG_OWNER.PS_UM_STRZ_HSGDTL
 where DATA_ORIGIN <> 'D'),
    Q2 as (
select Q1.INSTITUTION_CD, Q1.PERSON_ID, Q1.ACAD_YEAR_LD, Q1.SRC_SYS_ID,
       Q1.APPLIED_DT, Q1.COMPLETED_DT, Q1.AGREEMENT_DT,
       case when upper(HOUSING_TERM_LD) like 'FALL%'
            then to_char(to_number(substr(HOUSING_TERM_LD,8,2))+10)||'10'
            when upper(HOUSING_TERM_LD) like 'SPRING%'
            then to_char(to_number(substr(HOUSING_TERM_LD,10,2)+10))||'30'
       else '-' end HOUSING_TERM_CD,    -- Fall or Spring 
       HOUSING_TERM_LD, HOUSING_LOAD_DT
  from Q1),
    Q3 as (
select Q2.INSTITUTION_CD, Q2.PERSON_ID, Q2.ACAD_YEAR_LD, Q2.SRC_SYS_ID,
       Q2.APPLIED_DT, Q2.COMPLETED_DT, Q2.AGREEMENT_DT,
       substr(trim(Q2.HOUSING_TERM_LD),-4,4) HOUSING_TERM_CD,
       Q2.HOUSING_TERM_LD, Q2.HOUSING_LOAD_DT
  from Q2
 where Q2.HOUSING_TERM_CD = '-')    -- Full Year 
select Q2.INSTITUTION_CD, Q2.PERSON_ID, Q2.ACAD_YEAR_LD, Q2.HOUSING_TERM_CD, Q2.SRC_SYS_ID,
       nvl(I.INSTITUTION_SID,2147483646) INSTITUTION_SID,
       nvl(T.TERM_SID,2147483646) TERM_SID,
       nvl(P.PERSON_SID,2147483646) PERSON_SID,
       Q2.APPLIED_DT, Q2.COMPLETED_DT, Q2.AGREEMENT_DT,
       Q2.HOUSING_TERM_LD, Q2.HOUSING_LOAD_DT,
       'S' DATA_ORIGIN, SYSDATE CREATED_EW_DTTM, SYSDATE LASTUPD_EW_DTTM
  from Q2
  join CSMRT_OWNER.PS_D_INSTITUTION I
    on Q2.INSTITUTION_CD = I.INSTITUTION_CD
   and Q2.SRC_SYS_ID = I.SRC_SYS_ID
  join CSMRT_OWNER.PS_D_TERM T
    on Q2.INSTITUTION_CD = T.INSTITUTION_CD
   and 'UGRD' = T.ACAD_CAR_CD
   and Q2.HOUSING_TERM_CD = T.TERM_CD
   and Q2.SRC_SYS_ID = T.SRC_SYS_ID
  join CSMRT_OWNER.PS_D_PERSON P
    on Q2.PERSON_ID = P.PERSON_ID
   and Q2.SRC_SYS_ID = P.SRC_SYS_ID
 where Q2.HOUSING_TERM_CD <> '-'
 union all
select Q3.INSTITUTION_CD, Q3.PERSON_ID, Q3.ACAD_YEAR_LD, T.TERM_CD HOUSING_TERM_CD, Q3.SRC_SYS_ID,
       nvl(I.INSTITUTION_SID,2147483646) INSTITUTION_SID,
       nvl(T.TERM_SID,2147483646) TERM_SID,
       nvl(P.PERSON_SID,2147483646) PERSON_SID,
       APPLIED_DT, COMPLETED_DT, AGREEMENT_DT,
       T.TERM_LD HOUSING_TERM_LD, HOUSING_LOAD_DT,
       'S' DATA_ORIGIN, SYSDATE CREATED_EW_DTTM, SYSDATE LASTUPD_EW_DTTM
  from Q3
  join CSMRT_OWNER.PS_D_INSTITUTION I
    on Q3.INSTITUTION_CD = I.INSTITUTION_CD
   and Q3.SRC_SYS_ID = I.SRC_SYS_ID
  join CSMRT_OWNER.PS_D_TERM T
    on Q3.INSTITUTION_CD = T.INSTITUTION_CD
   and 'UGRD' = T.ACAD_CAR_CD
   and substr(T.TERM_CD,-2,2) in ('10','30')
   and Q3.HOUSING_TERM_CD = to_char(T.ACAD_YR_SID)
   and Q3.SRC_SYS_ID = T.SRC_SYS_ID
  join CSMRT_OWNER.PS_D_PERSON P
    on Q3.PERSON_ID = P.PERSON_ID
   and Q3.SRC_SYS_ID = P.SRC_SYS_ID
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_F_HOUSING rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_HOUSING',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Inserting data into CSMRT_OWNER.UM_F_HOUSING';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.UM_F_HOUSING';				

insert into CSMRT_OWNER.UM_F_HOUSING
select '-' INSTITUTION_CD, '-' PERSON_ID, '-' ACAD_YEAR_LD, '-' HOUSING_TERM_CD, 'CS90' SRC_SYS_ID, 
       2147483646 INSTITUTION_SID, 2147483646 TERM_SID, 2147483646 PERSON_SID, NULL APPLIED_DT, NULL COMPLETED_DT, NULL AGREEMENT_DT, '-' HOUSING_TERM_LD, NULL HOUSING_LOAD_DT, 
       'S' DATA_ORIGIN, SYSDATE CREATED_EW_DTTM, SYSDATE LASTUPD_EW_DTTM
  from DUAL
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_F_HOUSING rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_HOUSING',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.UM_F_HOUSING';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_HOUSING enable constraint PK_UM_F_HOUSING';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );
				
COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','UM_F_HOUSING');

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

END UM_F_HOUSING_P;
/
