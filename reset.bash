rm adm.db
mkdir data_backup
mkdir dev_backup

data_name=data_backup/data
if [[ -e $data_name ]] ; then
    i=0
    while [[ -e $data_name-$i ]] ; do
        let i++
    done
    data_name=$data_name-$i
fi

dev_name=dev_backup/dev
if [[ -e $dev_name ]] ; then
    i=0
    while [[ -e $dev_name-$i ]] ; do
        let i++
    done
    dev_name=$dev_name-$i
fi

mv data $data_name
mv dev $dev_name

# rm -r $data_name
# rm -r $dev_name
go build
echo "adm reset"