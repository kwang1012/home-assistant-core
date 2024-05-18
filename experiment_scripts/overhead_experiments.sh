# Run overhead experiments
# create 10 copies of the config file
estimations=(mean p50 p70 p80 p90 p95 p99)
triggers=(reactive anticipatory proactive)
dataset="${1:-morning}"
case $dataset in
    morning|afternoon|evening|all)
        # do nothing
        ;;
    *)
        # error
        echo 'dataset is not one of morning|afternoon|evening|all'
        exit 1
esac

rm -rf experiment_scripts/config-*

echo "Dataset: $dataset"
for i in ${!estimations[@]}
do
    for j in ${!triggers[@]}
    do
        echo "Running overhead experiment (${estimations[$i]}, ${triggers[$j]})"
        cp -r ./config ./experiment_scripts/config-${estimations[$i]}-${triggers[$j]}
        sed -i "s/action_length_estimation: mean/action_length_estimation: ${estimations[$i]}/g" ./experiment_scripts/config-${estimations[$i]}-${triggers[$j]}/rasc.yaml
        sed -i "s/rescheduling_trigger: proactive/rescheduling_trigger: ${triggers[$j]}/g" ./experiment_scripts/config-${estimations[$i]}-${triggers[$j]}/rasc.yaml
        sed -i "s/overhead_measurement: false/overhead_measurement: true/g" ./experiment_scripts/config-${estimations[$i]}-${triggers[$j]}/rasc.yaml
        hass -c ./experiment_scripts/config-${estimations[$i]}-${triggers[$j]}
        rm -rf ./experiment_scripts/config-${estimations[$i]}-${triggers[$j]}
    done
done

# Run baseline
echo "Running overhead experiment baseline"
cp -r ./config ./experiment_scripts/config-baseline
sed -i "s/enabled: true/enabled: false/g" ./experiment_scripts/config-baseline/rasc.yaml
sed -i "s/routine_arrival_filename: arrival_morning.csv/routine_arrival_filename: arrival_$dataset.csv/g" ./experiment_scripts/config-baseline/rasc.yaml
sed -i "s/overhead_measurement: false/overhead_measurement: true/g" ./experiment_scripts/config-baseline/rasc.yaml
hass -c ./experiment_scripts/config-baseline
rm -rf ./experiment_scripts/config-baseline