DROP PROCEDURE CSMRT_OWNER.UM_A_SRVC_IND_P
/

--
-- UM_A_SRVC_IND_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."UM_A_SRVC_IND_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads table UM_A_SRVC_IND.
--
 --V01  SMT-xxxx 07/02/2018,    James Doucette
--                              Converted from SQL Script
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_A_SRVC_IND';
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

strMessage01    := 'Truncating table CSMRT_OWNER.UM_A_SRVC_IND';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.UM_A_SRVC_IND';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.UM_A_SRVC_IND';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','UM_A_SRVC_IND');

--strSqlDynamic   := 'alter table CSMRT_OWNER.UM_A_SRVC_IND disable constraint PK_UM_A_SRVC_IND';
--strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
--COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
--                (
--                i_SqlStatement          => strSqlDynamic,
--                i_MaxTries              => 10,
--                i_WaitSeconds           => 10,
--                o_Tries                 => intTries
--                );
				
strMessage01    := 'Inserting data into CSMRT_OWNER.UM_A_SRVC_IND';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.UM_A_SRVC_IND';				
insert /*+ append enable_parallel_dml parallel(8) */ into UM_A_SRVC_IND
with XL as (
select /*+ PARALLEL(8) INLINE */ 
       FIELDNAME, FIELDVALUE, SRC_SYS_ID, XLATLONGNAME, XLATSHORTNAME
  from CSMRT_OWNER.UM_D_XLATITEM
 where SRC_SYS_ID = 'CS90'),
    CD as (
select /*+ PARALLEL(8) INLINE */ 
       distinct INSTITUTION_CD, SRVC_IND_CD, SRC_SYS_ID, 
       SRVC_IND_SD, SRVC_IND_LD 
  from UM_D_PERSON_SRVC_IND
 where DATA_ORIGIN <> 'D'),
   RSN as (
select /*+ PARALLEL(8) INLINE */ 
       distinct INSTITUTION_CD, SRVC_IND_CD, SRVC_IND_REASON, SRC_SYS_ID, 
       SRVC_IND_REASON_SD, SRVC_IND_REASON_LD
  from UM_D_PERSON_SRVC_IND
 where DATA_ORIGIN <> 'D')
select /*+ PARALLEL(8) */
       A.AUDIT_OPRID,    
       A.AUDIT_STAMP, 
       A.AUDIT_ACTN,    
       A.EMPLID PERSON_ID,   
       A.SRVC_IND_DTTM, 
       A.SRC_SYS_ID, 
       A.INSTITUTION INSTITUTION_CD,  
       I.INSTITUTION_SID,  
       P.PERSON_SID, 
       AMOUNT,
       nvl(X1.XLATSHORTNAME,'') AUDIT_ACTN_SD,
       nvl(X1.XLATLONGNAME,'') AUDIT_ACTN_LD,
       CONTACT,
       CONTACT_ID,
       DEPTID,
       OPRID,
       PLACED_METHOD,
       PLACED_PERSON,
       PLACED_PERSON_ID,
       PLACED_PROCESS,
       POS_SRVC_INDICATOR,
       PROCESS_INSTANCE,
       RELEASE_PROCESS,
       SCC_SI_END_TERM,
       SCC_SI_END_DT,
       SRVC_IND_ACT_TERM,
       SRVC_IND_ACTIVE_DT,
       SRVC_IND_REFRNCE,
       A.SRVC_IND_CD, 
       nvl(CD.SRVC_IND_SD,'-') SRVC_IND_SD, 
       nvl(CD.SRVC_IND_LD,'-') SRVC_IND_LD, 
       nvl(A.SRVC_IND_REASON,'-') SRVC_IND_REASON, 
       nvl(RSN.SRVC_IND_REASON_SD,'-') SRVC_IND_REASON_SD, 
       nvl(RSN.SRVC_IND_REASON_LD,'-') SRVC_IND_REASON_LD,
       COMM_COMMENTS,
       'S' DATA_ORIGIN, 
       SYSDATE CREATED_EW_DTTM, 
       SYSDATE LASTUPD_EW_DTTM 
  from CSSTG_OWNER.PS_AUDIT_SRVC_IND A
  join PS_D_INSTITUTION I
    on A.INSTITUTION = I.INSTITUTION_CD
   and A.SRC_SYS_ID = I.SRC_SYS_ID
  join CSMRT_OWNER.PS_D_PERSON P
    on A.EMPLID = P.PERSON_ID
   and A.SRC_SYS_ID = P.SRC_SYS_ID
  left outer join CD
    on A.INSTITUTION = CD.INSTITUTION_CD
   and A.SRVC_IND_CD = CD.SRVC_IND_CD
   and A.SRC_SYS_ID = CD.SRC_SYS_ID
  left outer join RSN 
    on A.INSTITUTION = RSN.INSTITUTION_CD
   and A.SRVC_IND_CD = RSN.SRVC_IND_CD
   and A.SRVC_IND_REASON = RSN.SRVC_IND_REASON
   and A.SRC_SYS_ID = RSN.SRC_SYS_ID
  left outer join XL X1
    on X1.FIELDNAME = 'AUDIT_ACTN'
   and X1.FIELDVALUE = A.AUDIT_ACTN 
 where A.DATA_ORIGIN <> 'D'
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_A_SRVC_IND rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_A_SRVC_IND',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Inserting data into CSMRT_OWNER.UM_A_SRVC_IND';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.UM_A_SRVC_IND';				
Insert into CSMRT_OWNER.UM_A_SRVC_IND
   (AUDIT_OPRID, AUDIT_STAMP, AUDIT_ACTN, PERSON_ID, SRVC_IND_DTTM, 
    SRC_SYS_ID, INSTITUTION_CD, INSTITUTION_SID, PERSON_SID, AMOUNT, 
    AUDIT_ACTN_SD, AUDIT_ACTN_LD, CONTACT, CONTACT_ID, DEPTID, 
    OPRID, PLACED_METHOD, PLACED_PERSON, PLACED_PERSON_ID, PLACED_PROCESS, 
    POS_SRVC_INDICATOR, PROCESS_INSTANCE, RELEASE_PROCESS, SCC_SI_END_TERM, SRVC_IND_ACT_TERM, 
    SRVC_IND_ACTIVE_DT, SRVC_IND_REFRNCE, SRVC_IND_CD, SRVC_IND_SD, SRVC_IND_LD, 
    SRVC_IND_REASON, SRVC_IND_REASON_SD, SRVC_IND_REASON_LD, DATA_ORIGIN, 
    CREATED_EW_DTTM, LASTUPD_EW_DTTM)
 Values
   ('-', TO_TIMESTAMP('01/01/1901 1:01:01.000000 PM','fmMMfm/fmDDfm/YYYY fmHH12fm:MI:SS.FF AM'), '-', '-', TO_TIMESTAMP('01/01/1901 1:01:01.000000 PM','fmMMfm/fmDDfm/YYYY fmHH12fm:MI:SS.FF AM'), 
    'CS90', '-', 2147483646, 2147483646, 0, 
    '-', '-', '-', '-', '-', 
    '-', '-', ' ', '-', '-', 
    '-', 0, ' ', ' ', '-', 
    TO_DATE('1/1/1901', 'MM/DD/YYYY'), ' ', '-', '-', 
    '-', '-', '-', '-', '-', 
    SYSDATE, SYSDATE);

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_A_SRVC_IND rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_A_SRVC_IND',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.UM_A_SRVC_IND';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
--alter table UM_A_SRVC_IND enable constraint PK_UM_A_SRVC_IND;

--strSqlDynamic   := 'alter table CSMRT_OWNER.UM_A_SRVC_IND enable constraint PK_UM_A_SRVC_IND';
--strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
--COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
--                (
--                i_SqlStatement          => strSqlDynamic,
--                i_MaxTries              => 10,
--                i_WaitSeconds           => 10,
--                o_Tries                 => intTries
--                );
				
COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','UM_A_SRVC_IND');

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

END UM_A_SRVC_IND_P;
/
