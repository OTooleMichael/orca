
echo "---- Starting Server ----"
echo ""

watchmedo auto-restart --recursive --pattern="*.py;*.yaml" \
	--signal SIGTERM \
	--kill-after 5 \
	python -- -u $1
	;

echo ""
echo "---- Server Stopped ----"
