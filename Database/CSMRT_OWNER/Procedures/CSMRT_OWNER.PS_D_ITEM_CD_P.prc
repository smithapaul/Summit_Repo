DROP PROCEDURE CSMRT_OWNER.PS_D_ITEM_CD_P
/

--
-- PS_D_ITEM_CD_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."PS_D_ITEM_CD_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_D_ITEM_CD from PeopleSoft table PS_D_ITEM_CD.
--
 --V01  SMT-xxxx 01/31/2019,    James Doucette
--                              Converted from SQL Script
--V02 2/12/2021            -- Srikanth,Pabbu made changes to ITEM_CD_SID field
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_D_ITEM_CD';
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

strMessage01    := 'Merging data into CSSTG_OWNER.PS_D_ITEM_CD';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSSTG_OWNER.PS_D_ITEM_CD';
merge /* parallel(T,8) */ /*+ use_hash(S,T) */ into CSMRT_OWNER.PS_D_ITEM_CD T
using (
with C as (
select CHKLST_ITEM_CD, SRC_SYS_ID, EFFDT, EFF_STATUS, ITEM_ASSOCIATION, DESCRSHORT, DESCR, 
row_number() over (partition by CHKLST_ITEM_CD, SRC_SYS_ID
                       order by decode(DATA_ORIGIN,'D',9,0), EFFDT desc) ITEM_ORDER
from CSSTG_OWNER.PS_SCC_CKLSITM_TBL), 
I as (
select /*+ materialize */ distinct   
       H.INSTITUTION, D.CHKLST_ITEM_CD
  from CSSTG_OWNER.PS_PERSON_CHECKLST H
  join CSSTG_OWNER.PS_PERSON_CHK_ITEM D 
    on H.COMMON_ID = D.COMMON_ID  
   and H.SEQ_3C = D.SEQ_3C
   and H.SRC_SYS_ID = D.SRC_SYS_ID
   and D.DATA_ORIGIN <> 'D'
 where H.DATA_ORIGIN <> 'D'),
S as (
select C.CHKLST_ITEM_CD, C.SRC_SYS_ID,
       C.EFFDT, C.EFF_STATUS EFF_STAT_CD, 
       nvl(C.ITEM_ASSOCIATION,'-') ITEM_ASSOCIATION, 
       C.DESCRSHORT ITEM_CD_SD, C.DESCR ITEM_CD_LD,
       nvl((select min(I.INSTITUTION) from I where C.CHKLST_ITEM_CD = I.CHKLST_ITEM_CD and I.INSTITUTION = 'UMBOS'),'-') UMBOS_INSTITUTION_CD,  
       nvl((select min(I.INSTITUTION) from I where C.CHKLST_ITEM_CD = I.CHKLST_ITEM_CD and I.INSTITUTION = 'UMDAR'),'-') UMDAR_INSTITUTION_CD,  
       nvl((select min(I.INSTITUTION) from I where C.CHKLST_ITEM_CD = I.CHKLST_ITEM_CD and I.INSTITUTION = 'UMLOW'),'-') UMLOW_INSTITUTION_CD  
  from C
 where C.ITEM_ORDER = 1)
select nvl(D.ITEM_CD_SID, --max(D.ITEM_CD_SID) over (partition by 1) +  This code does not ignore SID 2147483646 and below line will fix the issue and is added on 2/12/2021
 (select nvl(max(ITEM_CD_SID),0) from CSMRT_OWNER.PS_D_ITEM_CD where ITEM_CD_SID <> 2147483646) + 
       row_number() over (partition by 1 order by D.ITEM_CD_SID nulls first)) ITEM_CD_SID, 
       nvl(D.CHKLST_ITEM_CD, S.CHKLST_ITEM_CD) CHKLST_ITEM_CD, 
       nvl(D.SRC_SYS_ID, S.SRC_SYS_ID) SRC_SYS_ID, 
       decode(D.EFFDT,S.EFFDT,D.EFFDT,S.EFFDT) EFFDT,
       decode(D.EFF_STAT_CD,S.EFF_STAT_CD,D.EFF_STAT_CD,S.EFF_STAT_CD) EFF_STAT_CD,
       decode(D.ITEM_ASSOCIATION,S.ITEM_ASSOCIATION,D.ITEM_ASSOCIATION,S.ITEM_ASSOCIATION) ITEM_ASSOCIATION,
       decode(D.ITEM_CD_SD,S.ITEM_CD_SD,D.ITEM_CD_SD,S.ITEM_CD_SD) ITEM_CD_SD,
       decode(D.ITEM_CD_LD,S.ITEM_CD_LD,D.ITEM_CD_LD,S.ITEM_CD_LD) ITEM_CD_LD,
       decode(D.UMBOS_INSTITUTION_CD,S.UMBOS_INSTITUTION_CD,D.UMBOS_INSTITUTION_CD,S.UMBOS_INSTITUTION_CD) UMBOS_INSTITUTION_CD,
       decode(D.UMDAR_INSTITUTION_CD,S.UMDAR_INSTITUTION_CD,D.UMDAR_INSTITUTION_CD,S.UMDAR_INSTITUTION_CD) UMDAR_INSTITUTION_CD,
       decode(D.UMLOW_INSTITUTION_CD,S.UMLOW_INSTITUTION_CD,D.UMLOW_INSTITUTION_CD,S.UMLOW_INSTITUTION_CD) UMLOW_INSTITUTION_CD,
       nvl(D.LOAD_ERROR,'N') LOAD_ERROR, 
       nvl(D.DATA_ORIGIN,'S') DATA_ORIGIN, 
       nvl(D.CREATED_EW_DTTM, sysdate) CREATED_EW_DTTM, 
       nvl(D.LASTUPD_EW_DTTM, sysdate) LASTUPD_EW_DTTM, 
       nvl(D.BATCH_SID, 1234) BATCH_SID 
  from S
  left outer join CSMRT_OWNER.PS_D_ITEM_CD D 
    on D.CHKLST_ITEM_CD = S.CHKLST_ITEM_CD 
   and D.SRC_SYS_ID = S.SRC_SYS_ID 
   and D.ITEM_CD_SID < 2147483646) S 
   on (T.CHKLST_ITEM_CD = S.CHKLST_ITEM_CD
  and  T.SRC_SYS_ID = S.SRC_SYS_ID) 
 when matched then update set 
	T.EFFDT = S.EFFDT,
	T.EFF_STAT_CD = S.EFF_STAT_CD,
	T.ITEM_ASSOCIATION = S.ITEM_ASSOCIATION,
	T.ITEM_CD_SD = S.ITEM_CD_SD,
	T.ITEM_CD_LD = S.ITEM_CD_LD,
	T.UMBOS_INSTITUTION_CD = S.UMBOS_INSTITUTION_CD,
	T.UMDAR_INSTITUTION_CD = S.UMDAR_INSTITUTION_CD,
	T.UMLOW_INSTITUTION_CD = S.UMLOW_INSTITUTION_CD,
	T.DATA_ORIGIN = S.DATA_ORIGIN,
	T.LASTUPD_EW_DTTM = SYSDATE,
	T.BATCH_SID = S.BATCH_SID
where 
	decode(T.EFFDT,S.EFFDT,0,1) = 1 or
	decode(T.EFF_STAT_CD,S.EFF_STAT_CD,0,1) = 1 or
	decode(T.ITEM_ASSOCIATION,S.ITEM_ASSOCIATION,0,1) = 1 or
	decode(T.ITEM_CD_SD,S.ITEM_CD_SD,0,1) = 1 or
	decode(T.ITEM_CD_LD,S.ITEM_CD_LD,0,1) = 1 or
	decode(T.UMBOS_INSTITUTION_CD,S.UMBOS_INSTITUTION_CD,0,1) = 1 or
	decode(T.UMDAR_INSTITUTION_CD,S.UMDAR_INSTITUTION_CD,0,1) = 1 or
	decode(T.UMLOW_INSTITUTION_CD,S.UMLOW_INSTITUTION_CD,0,1) = 1 or
	decode(T.DATA_ORIGIN,S.DATA_ORIGIN,0,1) = 1 
 when not matched then
insert (
	T.ITEM_CD_SID,
	T.CHKLST_ITEM_CD,
	T.SRC_SYS_ID,
	T.EFFDT,
	T.EFF_STAT_CD,
	T.ITEM_ASSOCIATION,
	T.ITEM_CD_SD,
	T.ITEM_CD_LD,
	T.UMBOS_INSTITUTION_CD,
	T.UMDAR_INSTITUTION_CD,
	T.UMLOW_INSTITUTION_CD,
	T.LOAD_ERROR,
	T.DATA_ORIGIN,
	T.CREATED_EW_DTTM,
	T.LASTUPD_EW_DTTM,
	T.BATCH_SID)
	values (
	S.ITEM_CD_SID,
	S.CHKLST_ITEM_CD,
	S.SRC_SYS_ID,
	S.EFFDT,
	S.EFF_STAT_CD,
	S.ITEM_ASSOCIATION,
	S.ITEM_CD_SD,
	S.ITEM_CD_LD,
	S.UMBOS_INSTITUTION_CD,
	S.UMDAR_INSTITUTION_CD,
	S.UMLOW_INSTITUTION_CD,
	S.LOAD_ERROR,
	S.DATA_ORIGIN,
	SYSDATE,
	SYSDATE,
	S.BATCH_SID)
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_D_ITEM_CD rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_D_ITEM_CD',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_D_ITEM_CD';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSSTG_OWNER.PS_D_ITEM_CD';
update CSMRT_OWNER.PS_D_ITEM_CD T
   set DATA_ORIGIN = 'D',
       LASTUPD_EW_DTTM = sysdate,
       BATCH_SID = 1234
 where DATA_ORIGIN <> 'D'
   and T.ITEM_CD_SID < 2147483646
   and not exists (select 1
                     from CSSTG_OWNER.PS_SCC_CKLSITM_TBL S
                    where T.CHKLST_ITEM_CD = S.CHKLST_ITEM_CD
                      and T.SRC_SYS_ID = S.SRC_SYS_ID
                      and S.DATA_ORIGIN <> 'D')
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_D_ITEM_CD rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_D_ITEM_CD',
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

END PS_D_ITEM_CD_P;
/
