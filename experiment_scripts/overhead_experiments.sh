# for loop to run overhead experiments

# Run overhead experiments
# create 10 copies of the config file
estimations=(mean p50 p70 p80 p90 p95 p99)
rm -rf experiment_scripts/config-*
for i in ${!estimations[@]}
do
    echo "Running overhead experiment $i"
    cp -r ./config ./experiment_scripts/config-$i
    sed -i "s/mean/${estimations[$i]}/g" ./experiment_scripts/config-$i/rasc.yaml
    sed -i "s/overhead_measurement: false/overhead_measurement: true/g" ./experiment_scripts/config-$i/rasc.yaml
    hass -c ./experiment_scripts/config-$i
    rm -rf ./experiment_scripts/config-$i
done