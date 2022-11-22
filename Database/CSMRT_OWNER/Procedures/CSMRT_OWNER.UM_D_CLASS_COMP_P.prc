DROP PROCEDURE CSMRT_OWNER.UM_D_CLASS_COMP_P
/

--
-- UM_D_CLASS_COMP_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."UM_D_CLASS_COMP_P" AUTHID CURRENT_USER IS


------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table UM_D_CLASS_COMP from PeopleSoft table UM_D_CLASS_COMP.
--
 --V01  SMT-7588 02/01/2018,    James Doucette
--                              New Dimension
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_D_CLASS_COMP';
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



strMessage01    := 'Merging data into CSSTG_OWNER.UM_D_CLASS_COMP';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSSTG_OWNER.UM_D_CLASS_COMP';
merge /*+ use_hash(S,T) */ into CSMRT_OWNER.UM_D_CLASS_COMP T
using (
with Q1 as (
select 
	 CRSE_ID CRSE_CD, CRSE_OFFER_NBR CRSE_OFFER_NUM, STRM TERM_CD, SESSION_CODE SESSION_CD, ASSOCIATED_CLASS, SSR_COMPONENT SSR_COMP_CD, SRC_SYS_ID,
	 OPTIONAL_SECTION, CONTACT_HOURS, FINAL_EXAM, AUTO_CREATE_CMPNT, WEEK_WORKLOAD_HRS, 
	 DATA_ORIGIN
from CSSTG_OWNER.PS_CLASS_COMPONENT), 
 S as (
select 
	 C.CRSE_CD, C.CRSE_OFFER_NUM, C.TERM_CD, C.SESSION_CD, C.ASSOCIATED_CLASS, 
	 nvl(Q1.SSR_COMP_CD,'-') SSR_COMP_CD, C.SRC_SYS_ID, 
	 C.CLASS_ASSOC_SID, 
	 nvl(S.SSR_COMP_SID, 2147483646) SSR_COMP_SID, 
	 Q1.OPTIONAL_SECTION, Q1.CONTACT_HOURS, Q1.FINAL_EXAM, Q1.AUTO_CREATE_CMPNT, Q1.WEEK_WORKLOAD_HRS, 
	 least(C.DATA_ORIGIN,nvl(Q1.DATA_ORIGIN,'Z')) DATA_ORIGIN
from CSMRT_OWNER.UM_D_CLASS_ASSOC C
left outer join Q1 
	  on C.CRSE_CD = Q1.CRSE_CD
	 and C.CRSE_OFFER_NUM = Q1.CRSE_OFFER_NUM
	 and C.TERM_CD = Q1.TERM_CD
	 and C.SESSION_CD = Q1.SESSION_CD
	 and C.ASSOCIATED_CLASS = Q1.ASSOCIATED_CLASS 
	 and C.SRC_SYS_ID = Q1.SRC_SYS_ID
left outer join PS_D_SSR_COMP S -- _NEW!!!!!!!!!!!!!!! 
	  on nvl(Q1.SSR_COMP_CD,'-') = S.SSR_COMP_CD
	 and Q1.SRC_SYS_ID = S.SRC_SYS_ID
) 
select 
     nvl(D.CRSE_CD, S.CRSE_CD) CRSE_CD, 
	 nvl(D.CRSE_OFFER_NUM, S.CRSE_OFFER_NUM) CRSE_OFFER_NUM, 
	 nvl(D.TERM_CD, S.TERM_CD) TERM_CD,
	 nvl(D.SESSION_CD, S.SESSION_CD) SESSION_CD, 
	 nvl(D.ASSOCIATED_CLASS, S.ASSOCIATED_CLASS) ASSOCIATED_CLASS, 
	 nvl(D.SSR_COMP_CD, S.SSR_COMP_CD) SSR_COMP_CD,
	 nvl(D.SRC_SYS_ID, S.SRC_SYS_ID) SRC_SYS_ID, 
	 decode(D.CLASS_ASSOC_SID, S.CLASS_ASSOC_SID, D.CLASS_ASSOC_SID, S.CLASS_ASSOC_SID) CLASS_ASSOC_SID, 
	 decode(D.SSR_COMP_SID, S.SSR_COMP_SID, D.SSR_COMP_SID, S.SSR_COMP_SID) SSR_COMP_SID,
	 decode(D.OPTIONAL_SECTION, S.OPTIONAL_SECTION, D.OPTIONAL_SECTION, S.OPTIONAL_SECTION) OPTIONAL_SECTION,
	 decode(D.CONTACT_HOURS, S.CONTACT_HOURS, D.CONTACT_HOURS, S.CONTACT_HOURS) CONTACT_HOURS, 
	 decode(D.FINAL_EXAM, S.FINAL_EXAM, D.FINAL_EXAM, S.FINAL_EXAM) FINAL_EXAM,
	 decode(D.AUTO_CREATE_CMPNT, S.AUTO_CREATE_CMPNT, D.AUTO_CREATE_CMPNT, S.AUTO_CREATE_CMPNT) AUTO_CREATE_CMPNT, 
	 decode(D.WEEK_WORKLOAD_HRS, S.WEEK_WORKLOAD_HRS, D.WEEK_WORKLOAD_HRS, S.WEEK_WORKLOAD_HRS) WEEK_WORKLOAD_HRS, 
	 decode(D.DATA_ORIGIN, S.DATA_ORIGIN, D.DATA_ORIGIN, S.DATA_ORIGIN) DATA_ORIGIN, 
	 nvl(D.CREATED_EW_DTTM, SYSDATE) CREATED_EW_DTTM,
	 nvl(D.LASTUPD_EW_DTTM, SYSDATE) LASTUPD_EW_DTTM 
from S 
left outer join CSMRT_OWNER.UM_D_CLASS_COMP D
	  on D.CRSE_CD <> '-' 
	 and D.CRSE_CD = S.CRSE_CD     
	 and D.CRSE_OFFER_NUM = S.CRSE_OFFER_NUM 
	 and D.TERM_CD = S.TERM_CD 
	 and D.SESSION_CD = S.SESSION_CD 
	 and D.ASSOCIATED_CLASS = S.ASSOCIATED_CLASS 
	 and D.SSR_COMP_CD = S.SSR_COMP_CD 
	 and D.SRC_SYS_ID = S.SRC_SYS_ID 
) S
on(  T.CRSE_CD = S.CRSE_CD      
 and T.CRSE_OFFER_NUM = S.CRSE_OFFER_NUM 
 and T.TERM_CD = S.TERM_CD
 and T.SESSION_CD = S.SESSION_CD
 and T.ASSOCIATED_CLASS = S.ASSOCIATED_CLASS
 and T.SSR_COMP_CD = S.SSR_COMP_CD
 and T.SRC_SYS_ID = S.SRC_SYS_ID) 
 when matched then update set
	 T.CLASS_ASSOC_SID = S.CLASS_ASSOC_SID,
	 T.SSR_COMP_SID = S.SSR_COMP_SID,
	 T.OPTIONAL_SECTION = S.OPTIONAL_SECTION,
	 T.CONTACT_HOURS = S.CONTACT_HOURS,
	 T.FINAL_EXAM = S.FINAL_EXAM,
	 T.AUTO_CREATE_CMPNT = S.AUTO_CREATE_CMPNT,
	 T.WEEK_WORKLOAD_HRS = S.WEEK_WORKLOAD_HRS,
	 T.DATA_ORIGIN = S.DATA_ORIGIN,
	 T.LASTUPD_EW_DTTM = SYSDATE 
 where 
	 decode(T.CLASS_ASSOC_SID,S.CLASS_ASSOC_SID,0,1) = 1 or
	 decode(T.SSR_COMP_SID,S.SSR_COMP_SID,0,1) = 1 or
	 decode(T.OPTIONAL_SECTION,S.OPTIONAL_SECTION,0,1) = 1 or
	 decode(T.CONTACT_HOURS,S.CONTACT_HOURS,0,1) = 1 or
	 decode(T.FINAL_EXAM,S.FINAL_EXAM,0,1) = 1 or
	 decode(T.AUTO_CREATE_CMPNT,S.AUTO_CREATE_CMPNT,0,1) = 1 or
	 decode(T.WEEK_WORKLOAD_HRS,S.WEEK_WORKLOAD_HRS,0,1) = 1 or
	 decode(T.DATA_ORIGIN,S.DATA_ORIGIN,0,1) = 1 
when not matched then
insert ( 
	 T.CRSE_CD,
	 T.CRSE_OFFER_NUM, 
	 T.TERM_CD,
	 T.SESSION_CD, 
	 T.ASSOCIATED_CLASS, 
	 T.SSR_COMP_CD,
	 T.SRC_SYS_ID, 
	 T.CLASS_ASSOC_SID,
	 T.SSR_COMP_SID, 
	 T.OPTIONAL_SECTION, 
	 T.CONTACT_HOURS,
	 T.FINAL_EXAM, 
	 T.AUTO_CREATE_CMPNT,
	 T.WEEK_WORKLOAD_HRS,
	 T.DATA_ORIGIN,
	 T.CREATED_EW_DTTM,
	 T.LASTUPD_EW_DTTM)
values ( 
	 S.CRSE_CD,
	 S.CRSE_OFFER_NUM, 
	 S.TERM_CD,
	 S.SESSION_CD, 
	 S.ASSOCIATED_CLASS, 
	 S.SSR_COMP_CD,
	 S.SRC_SYS_ID, 
	 S.CLASS_ASSOC_SID,
	 S.SSR_COMP_SID, 
	 S.OPTIONAL_SECTION, 
	 S.CONTACT_HOURS,
	 S.FINAL_EXAM, 
	 S.AUTO_CREATE_CMPNT,
	 S.WEEK_WORKLOAD_HRS,
	 S.DATA_ORIGIN,
	 SYSDATE,
	 SYSDATE)
; 

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_D_CLASS_COMP rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_D_CLASS_COMP',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating DATA_ORIGIN on CSSTG_OWNER.UM_D_CLASS_COMP';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSSTG_OWNER.UM_D_CLASS_COMP';
update CSMRT_OWNER.UM_D_CLASS_COMP T
   set DATA_ORIGIN = 'D',
       LASTUPD_EW_DTTM = SYSDATE
 where DATA_ORIGIN <> 'D'
--   and T.GPA_TYPE_SID < 2147483646
   and not exists (select 1
                     from CSMRT_OWNER.UM_D_CLASS_ASSOC S  
                    where T.CRSE_CD = S.CRSE_CD
                      and T.CRSE_OFFER_NUM = S.CRSE_OFFER_NUM
                      and T.TERM_CD = S.TERM_CD
                      and T.SESSION_CD = S.SESSION_CD
                      and T.ASSOCIATED_CLASS = S.ASSOCIATED_CLASS
                      and T.SRC_SYS_ID = S.SRC_SYS_ID
					  )
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_D_CLASS_COMP rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_D_CLASS_COMP',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


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

END UM_D_CLASS_COMP_P;
/
