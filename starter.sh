echo "---- Starting Server ----"
echo ""
python generate_grpc.py

watchmedo auto-restart --recursive --pattern="*.py;*.yaml" \
	--interval=3 \
	--signal SIGTERM \
	--kill-after 5 \
	python -- -u $1 \
	;

echo ""
echo "---- Server Stopped ----"
