ALTER TABLE CSSTG_OWNER.PS_S_TREENODE_OLD MODIFY 
  PARENT_NODE_NUM NULL
/

ALTER TABLE CSSTG_OWNER.PS_S_TREENODE_OLD MODIFY 
  PARENT_NODE_NUM NOT NULL
  ENABLE VALIDATE
/
