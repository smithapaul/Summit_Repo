DROP PROCEDURE CSMRT_OWNER.PS_ITEM_TYPE_FISCL_P
/

--
-- PS_ITEM_TYPE_FISCL_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."PS_ITEM_TYPE_FISCL_P"
   AUTHID CURRENT_USER
IS
   /*
   -- Run before the first time
   DELETE
   FROM CSSTG_OWNER.UM_STAGE_JOBS
    WHERE TABLE_NAME = 'PS_ITEM_TYPE_FISCL'

   INSERT INTO CSSTG_OWNER.UM_STAGE_JOBS
   (TABLE_NAME, DELETE_FLG)
   VALUES
   ('PS_ITEM_TYPE_FISCL', 'Y')

   SELECT *
   FROM CSSTG_OWNER.UM_STAGE_JOBS
    WHERE TABLE_NAME = 'PS_ITEM_TYPE_FISCL'
   */


   ------------------------------------------------------------------------
   -- George Adams
   --
   -- Loads stage table PS_ITEM_TYPE_FISCL from PeopleSoft table PS_ITEM_TYPE_FISCL.
   --
   -- V01  SMT-xxxx 08/08/2017,    Jim Doucette
   --                              Converted from PS_ITEM_TYPE_FISCL.sql
   --
   ------------------------------------------------------------------------

   strMartId          VARCHAR2 (50) := 'CSW';
   strProcessName     VARCHAR2 (100) := 'PS_ITEM_TYPE_FISCL';
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

   strMessage01 := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);


   strSqlCommand := 'update START_DT on CSSTG_OWNER.UM_STAGE_JOBS';

   UPDATE CSSTG_OWNER.UM_STAGE_JOBS
      SET TABLE_STATUS = 'Reading', START_DT = SYSDATE, END_DT = NULL
    WHERE TABLE_NAME = 'PS_ITEM_TYPE_FISCL';

   strSqlCommand := 'commit';
   COMMIT;


   strSqlCommand := 'update NEW_MAX_SCN on CSSTG_OWNER.UM_STAGE_JOBS';

   UPDATE CSSTG_OWNER.UM_STAGE_JOBS
      SET TABLE_STATUS = 'Merging',
          NEW_MAX_SCN =
             (SELECT /*+ full(S) */
                    MAX (ORA_ROWSCN)
                FROM SYSADM.PS_ITEM_TYPE_FISCL@SASOURCE S)
    WHERE TABLE_NAME = 'PS_ITEM_TYPE_FISCL';

   strSqlCommand := 'commit';
   COMMIT;


   strMessage01 := 'Merging data into CSSTG_OWNER.PS_ITEM_TYPE_FISCL';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'merge into CSSTG_OWNER.PS_ITEM_TYPE_FISCL';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.PS_ITEM_TYPE_FISCL T
using (select /*+ full(S) */
    nvl(trim(SETID),'-') SETID, 
    nvl(trim(ITEM_TYPE),'-') ITEM_TYPE, 
    nvl(trim(AID_YEAR),'-') AID_YEAR, 
    nvl(trim(INSTITUTION),'-') INSTITUTION, 
    nvl(MAX_OFR_BUDGT,0) MAX_OFR_BUDGT, 
    nvl(OFR_GROSS,0) OFR_GROSS, 
    nvl(OFR_REDUCTN,0) OFR_REDUCTN, 
    nvl(OFR_NET,0) OFR_NET, 
    nvl(OFR_AVAILABLE,0) OFR_AVAILABLE, 
    nvl(OFFERED_NET_COUNT,0) OFFERED_NET_COUNT, 
    nvl(MAX_ACC_BUDGT,0) MAX_ACC_BUDGT, 
    nvl(ACC_GROSS,0) ACC_GROSS, 
    nvl(ACC_REDUCTN,0) ACC_REDUCTN, 
    nvl(ACC_NET,0) ACC_NET, 
    nvl(ACC_AVAILABLE,0) ACC_AVAILABLE, 
    nvl(ACCEPTED_NET_COUNT,0) ACCEPTED_NET_COUNT, 
    nvl(DECLINED_AMTS,0) DECLINED_AMTS, 
    nvl(DECLINED_COUNT,0) DECLINED_COUNT, 
    nvl(CANCELLED_AMTS,0) CANCELLED_AMTS, 
    nvl(CANCELLED_COUNT,0) CANCELLED_COUNT, 
    nvl(MAX_ATH_BUDGT,0) MAX_ATH_BUDGT, 
    nvl(NET_ATH_AMT,0) NET_ATH_AMT, 
    nvl(MAX_DSB_BUDGT,0) MAX_DSB_BUDGT, 
    nvl(NET_DSB_PAID,0) NET_DSB_PAID, 
    nvl(POTENTIAL_DSB,0) POTENTIAL_DSB, 
    nvl(trim(CURRENCY_CD),'-') CURRENCY_CD, 
    nvl(HIGHEST_ACC_SUM,0) HIGHEST_ACC_SUM, 
    nvl(HIGHEST_OFR_SUM,0) HIGHEST_OFR_SUM
from SYSADM.PS_ITEM_TYPE_FISCL@SASOURCE S 
where ORA_ROWSCN > (select OLD_MAX_SCN from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_ITEM_TYPE_FISCL') ) S
 on ( 
    T.SETID = S.SETID and 
    T.ITEM_TYPE = S.ITEM_TYPE and 
    T.AID_YEAR = S.AID_YEAR and 
    T.SRC_SYS_ID = 'CS90')
when matched then update set
    T.INSTITUTION = S.INSTITUTION,
    T.MAX_OFR_BUDGT = S.MAX_OFR_BUDGT,
    T.OFR_GROSS = S.OFR_GROSS,
    T.OFR_REDUCTN = S.OFR_REDUCTN,
    T.OFR_NET = S.OFR_NET,
    T.OFR_AVAILABLE = S.OFR_AVAILABLE,
    T.OFFERED_NET_COUNT = S.OFFERED_NET_COUNT,
    T.MAX_ACC_BUDGT = S.MAX_ACC_BUDGT,
    T.ACC_GROSS = S.ACC_GROSS,
    T.ACC_REDUCTN = S.ACC_REDUCTN,
    T.ACC_NET = S.ACC_NET,
    T.ACC_AVAILABLE = S.ACC_AVAILABLE,
    T.ACCEPTED_NET_COUNT = S.ACCEPTED_NET_COUNT,
    T.DECLINED_AMTS = S.DECLINED_AMTS,
    T.DECLINED_COUNT = S.DECLINED_COUNT,
    T.CANCELLED_AMTS = S.CANCELLED_AMTS,
    T.CANCELLED_COUNT = S.CANCELLED_COUNT,
    T.MAX_ATH_BUDGT = S.MAX_ATH_BUDGT,
    T.NET_ATH_AMT = S.NET_ATH_AMT,
    T.MAX_DSB_BUDGT = S.MAX_DSB_BUDGT,
    T.NET_DSB_PAID = S.NET_DSB_PAID,
    T.POTENTIAL_DSB = S.POTENTIAL_DSB,
    T.CURRENCY_CD = S.CURRENCY_CD,
    T.HIGHEST_ACC_SUM = S.HIGHEST_ACC_SUM,
    T.HIGHEST_OFR_SUM = S.HIGHEST_OFR_SUM,
    T.DATA_ORIGIN = 'S',
    T.LASTUPD_EW_DTTM = sysdate,
    T.BATCH_SID = 1234
where 
    T.INSTITUTION <> S.INSTITUTION or 
    T.MAX_OFR_BUDGT <> S.MAX_OFR_BUDGT or 
    T.OFR_GROSS <> S.OFR_GROSS or 
    T.OFR_REDUCTN <> S.OFR_REDUCTN or 
    T.OFR_NET <> S.OFR_NET or 
    T.OFR_AVAILABLE <> S.OFR_AVAILABLE or 
    T.OFFERED_NET_COUNT <> S.OFFERED_NET_COUNT or 
    T.MAX_ACC_BUDGT <> S.MAX_ACC_BUDGT or 
    T.ACC_GROSS <> S.ACC_GROSS or 
    T.ACC_REDUCTN <> S.ACC_REDUCTN or 
    T.ACC_NET <> S.ACC_NET or 
    T.ACC_AVAILABLE <> S.ACC_AVAILABLE or 
    T.ACCEPTED_NET_COUNT <> S.ACCEPTED_NET_COUNT or 
    T.DECLINED_AMTS <> S.DECLINED_AMTS or 
    T.DECLINED_COUNT <> S.DECLINED_COUNT or 
    T.CANCELLED_AMTS <> S.CANCELLED_AMTS or 
    T.CANCELLED_COUNT <> S.CANCELLED_COUNT or 
    T.MAX_ATH_BUDGT <> S.MAX_ATH_BUDGT or 
    T.NET_ATH_AMT <> S.NET_ATH_AMT or 
    T.MAX_DSB_BUDGT <> S.MAX_DSB_BUDGT or 
    T.NET_DSB_PAID <> S.NET_DSB_PAID or 
    T.POTENTIAL_DSB <> S.POTENTIAL_DSB or 
    T.CURRENCY_CD <> S.CURRENCY_CD or 
    T.HIGHEST_ACC_SUM <> S.HIGHEST_ACC_SUM or 
    T.HIGHEST_OFR_SUM <> S.HIGHEST_OFR_SUM or 
    T.DATA_ORIGIN = 'D' 
when not matched then 
insert (
    T.SETID,
    T.ITEM_TYPE,
    T.AID_YEAR, 
    T.SRC_SYS_ID, 
    T.INSTITUTION,
    T.MAX_OFR_BUDGT,
    T.OFR_GROSS,
    T.OFR_REDUCTN,
    T.OFR_NET,
    T.OFR_AVAILABLE,
    T.OFFERED_NET_COUNT,
    T.MAX_ACC_BUDGT,
    T.ACC_GROSS,
    T.ACC_REDUCTN,
    T.ACC_NET,
    T.ACC_AVAILABLE,
    T.ACCEPTED_NET_COUNT, 
    T.DECLINED_AMTS,
    T.DECLINED_COUNT, 
    T.CANCELLED_AMTS, 
    T.CANCELLED_COUNT,
    T.MAX_ATH_BUDGT,
    T.NET_ATH_AMT,
    T.MAX_DSB_BUDGT,
    T.NET_DSB_PAID, 
    T.POTENTIAL_DSB,
    T.CURRENCY_CD,
    T.HIGHEST_ACC_SUM,
    T.HIGHEST_OFR_SUM,
    T.LOAD_ERROR, 
    T.DATA_ORIGIN,
    T.CREATED_EW_DTTM,
    T.LASTUPD_EW_DTTM,
    T.BATCH_SID
    ) 
values (
    S.SETID,
    S.ITEM_TYPE,
    S.AID_YEAR, 
    'CS90', 
    S.INSTITUTION,
    S.MAX_OFR_BUDGT,
    S.OFR_GROSS,
    S.OFR_REDUCTN,
    S.OFR_NET,
    S.OFR_AVAILABLE,
    S.OFFERED_NET_COUNT,
    S.MAX_ACC_BUDGT,
    S.ACC_GROSS,
    S.ACC_REDUCTN,
    S.ACC_NET,
    S.ACC_AVAILABLE,
    S.ACCEPTED_NET_COUNT, 
    S.DECLINED_AMTS,
    S.DECLINED_COUNT, 
    S.CANCELLED_AMTS, 
    S.CANCELLED_COUNT,
    S.MAX_ATH_BUDGT,
    S.NET_ATH_AMT,
    S.MAX_DSB_BUDGT,
    S.NET_DSB_PAID, 
    S.POTENTIAL_DSB,
    S.CURRENCY_CD,
    S.HIGHEST_ACC_SUM,
    S.HIGHEST_OFR_SUM,
    'N',
    'S',
    sysdate,
    sysdate,
    1234)
;

   strSqlCommand := 'SET intRowCount';
   intRowCount := SQL%ROWCOUNT;

   strSqlCommand := 'commit';
   COMMIT;

   strMessage01 :=
         '# of PS_ITEM_TYPE_FISCL rows merged: '
      || TO_CHAR (intRowCount, '999,999,999,999');
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
   COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL (
      i_TargetTableName   => 'PS_ITEM_TYPE_FISCL',
      i_Action            => 'MERGE',
      i_RowCount          => intRowCount);


   strMessage01 := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';

   UPDATE CSSTG_OWNER.UM_STAGE_JOBS
      SET TABLE_STATUS = 'Deleting', OLD_MAX_SCN = NEW_MAX_SCN
    WHERE TABLE_NAME = 'PS_ITEM_TYPE_FISCL';

   strSqlCommand := 'commit';
   COMMIT;


   strMessage01 := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_ITEM_TYPE_FISCL';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'update DATA_ORIGIN on CSSTG_OWNER.PS_ITEM_TYPE_FISCL';
update CSSTG_OWNER.PS_ITEM_TYPE_FISCL T
        set T.DATA_ORIGIN = 'D',
               T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select SETID, ITEM_TYPE, AID_YEAR
   from CSSTG_OWNER.PS_ITEM_TYPE_FISCL T2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_ITEM_TYPE_FISCL') = 'Y'
  minus
 select SETID, ITEM_TYPE, AID_YEAR
   from SYSADM.PS_ITEM_TYPE_FISCL@SASOURCE
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_ITEM_TYPE_FISCL') = 'Y'
   ) S
 where T.SETID = S.SETID
   and T.ITEM_TYPE = S.ITEM_TYPE
   and T.AID_YEAR = S.AID_YEAR
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

   strSqlCommand := 'SET intRowCount';
   intRowCount := SQL%ROWCOUNT;

   strSqlCommand := 'commit';
   COMMIT;

   strMessage01 :=
         '# of PS_ITEM_TYPE_FISCL rows updated: '
      || TO_CHAR (intRowCount, '999,999,999,999');
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
   COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL (
      i_TargetTableName   => 'PS_ITEM_TYPE_FISCL',
      i_Action            => 'UPDATE',
      i_RowCount          => intRowCount);


   strMessage01 := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

   UPDATE CSSTG_OWNER.UM_STAGE_JOBS
      SET TABLE_STATUS = 'Complete', END_DT = SYSDATE
    WHERE TABLE_NAME = 'PS_ITEM_TYPE_FISCL';

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
END PS_ITEM_TYPE_FISCL_P;
/
