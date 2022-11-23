DROP FUNCTION CSMRT_OWNER.GET_PRE_REQ_MET_FLG
/

--
-- GET_PRE_REQ_MET_FLG  (Function) 
--
CREATE OR REPLACE function CSMRT_OWNER.GET_PRE_REQ_MET_FLG (IN_SQL CLOB) 
return varchar2 
is OUT_VAL varchar2(100);
     V_SQL1 varchar2(100);
     V_SQL2 CLOB;
begin

V_SQL1 := 'select ''Y'' PRE_REQ_MET_FLG from DUAL where 1 = 1 ';
V_SQL2 := V_SQL1||IN_SQL;

execute immediate V_SQL2
   into OUT_VAL;

return OUT_VAL;

exception

when NO_DATA_FOUND then return 'N';

when OTHERS then return 'E';
        
end;
/
