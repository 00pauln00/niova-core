mkdir ./temp/

echo "Create DIR for logs and metadata"
cd temp
mkdir logs
mkdir code_cov
mkdir bins
export GOCOVERDIR="/home/$USER/temp/code_cov"

echo "${GOCOVERDIR}"

echo "Clone niova repo"
git clone git@github.com:00pauln00/niova-core.git
cd ~/temp/niova-core/ && git checkout code_coverage && git pull origin code_coverage

cd ~/temp/
git clone git@github.com:00pauln00/holon.git
cd ~/temp/holon/ && git checkout code_cover && git pull origin code_cover

echo "Compile niova-core"
cd ~/temp/niova-core && ./prepare.sh && ./configure --prefix=/home/$USER/temp/bins --enable-devel &&
        make clean && make && make install

cd ~/temp/bins
export LD_LIBRARY_PATH=$(pwd)

echo "Build go applications"
cd ~/temp/niova-core/go/controlPlane &&
        cd ncpc/ && go mod tidy && cd .. &&
        cd pmdbServer && go mod tidy && cd .. &&
        cd proxy && go mod tidy && cd .. &&
        make install_only -e DIR=/home/$USER/temp/bins/

cd ~/temp/niova-core/go/pumiceDB/examples/niovaKV &&
	make -e DIR=/home/$USER/temp/bins

echo "Copy run_recipes.sh"
cp ~/temp/niova-core/scripts/run-recipes.sh ~/temp/holon/

echo "Run controlplane recipes"
cd ~/temp/holon/ && ./run-recipes.sh "/home/$USER/temp/holon" "/home/$USER/temp/bins" "/home/$USER/temp/logs" 5 "/home/$USER/temp/niova-core/scripts/control_plane_recipes.txt" "controlplane" "/usr/local/go/bin" 5

echo "Run covid-app recipes"
cd ~/temp/holon/ && ./run-recipes.sh "/home/$USER/temp/holon" "/home/$USER/temp/bins" "/home/$USER/temp/log" 5 "/home/$USER/temp/niova-core/scripts/covid_app_recipe.txt" "covid" "0" "0" "/usr/local/go/bin"
cd ~/temp/holon/ && ./run-recipes.sh "/home/$USER/temp/holon" "/home/$USER/temp/bins" "/home/$USER/temp/log" 5 "/home/$USER/temp/niova-core/scripts/covid_app_recipe.txt" "foodpalace" "0" "0" "/usr/local/go/bin"
cd ~/temp/holon/ && ./run-recipes.sh "/home/$USER/temp/holon" "/home/$USER/temp/bins" "/home/$USER/temp/log" 5 "/home/$USER/temp/niova-core/scripts/covid_app_recipe.txt" "niovakv" "0" "0" "/usr/local/go/bin"

echo "Run covdata tool"
cd ~/temp/code_cov
RES=$(go tool covdata percent -i=./ | tail -1)
echo "${RES}"

touch ~/temp/code_cov/total_code_cov
echo "${RES}" > ~/temp/code_cov/total_code_cov

echo "Export data to influxDB"
python3 ~/temp/niova-core/scripts/load_cover_data.py ~/temp/code_cov/total_code_cov
