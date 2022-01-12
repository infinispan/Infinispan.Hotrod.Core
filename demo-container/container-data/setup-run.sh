git clone -b query-demo https://github.com/infinispan/Infinispan.Hotrod.Core.git || echo Infinspan.Hotrod.Core folder not empty. git clone failed. Continue...
export ISPN_VER=13.0.5.Final
wget -N https://downloads.jboss.org/infinispan/$ISPN_VER/infinispan-server-$ISPN_VER.zip
[ ! -d "infinispan-server-$ISPN_VER" ] && unzip -n infinispan-server-$ISPN_VER.zip || echo infinispan-server-$ISPN_VER folder not empty, no unzip. Continue...
cp -n Infinispan.Hotrod.Core/conf/infinispan-app.xml infinispan-server-$ISPN_VER/server/conf/
infinispan-server-$ISPN_VER/bin/server.sh -c infinispan-app.xml > console.log 2>&1 &
cd Infinispan.Hotrod.Core/
dotnet build
dotnet run

