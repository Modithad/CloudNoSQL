package fr.lri.cloudnosql.distrib;

import java.io.IOException;

public class DistributionHandler {
	private ZooKeeperConnection zooCon;

	public DistributionHandler(String zkHost) throws IOException, InterruptedException {
		zooCon = new ZooKeeperConnection();
		zooCon.connect(zkHost);
	}

}
