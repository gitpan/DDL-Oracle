copy_user.pl > copy_user.sql

if [ $? -gt 0 ]
then
  exit 1
fi

sqlplus -s /  << EOF | tee copy_user.log
  @ copy_user.sql
EOF

