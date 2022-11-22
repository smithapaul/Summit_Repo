DROP VIEW CSMRT_OWNER.UM_F_FA_AWARD_DISB_VW
/

--
-- UM_F_FA_AWARD_DISB_VW  (View) 
--
CREATE OR REPLACE VIEW CSMRT_OWNER.UM_F_FA_AWARD_DISB_VW
BEQUEATH DEFINER
AS 
SELECT 
           INSTITUTION_CD,
           ACAD_CAR_CD,
           AID_YEAR,
           TERM_CD,
           PERSON_ID,
           ITEM_TYPE,
           SRC_SYS_ID,
           PERSON_SID,
           INSTITUTION_SID,
           ACAD_CAR_SID,
           TERM_SID,
           ITEM_TYPE_SID,
           OFFER_BALANCE,
           ACCEPT_BALANCE,
           AUTHORIZED_BALANCE,
           DISBURSED_BALANCE,
           NET_DISB_BALANCE,
           NET_AWARD_AMT_SF
      FROM UM_F_FA_AWARD_DISB       --!!!!!!!!!!!!!!!!!!!
/
