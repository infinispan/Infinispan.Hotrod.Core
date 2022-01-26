PROJ_ROOT=$(git rev-parse --show-toplevel)
if [ -z $ISPN_HOME ]; then
    ISPN_VER=${ISPN_VER:='13.0.5.Final'}
    wget -q -N https://downloads.jboss.org/infinispan/$ISPN_VER/infinispan-server-$ISPN_VER.zip
    unzip -n infinispan-server-$ISPN_VER.zip
    ISPN_HOME=$(echo $PROJ_ROOT/infinispan-server-$ISPN_VER)
    echo "ISPN_HOME=$ISPN_HOME"
fi
cp -r ./conf/* $ISPN_HOME/server/conf
