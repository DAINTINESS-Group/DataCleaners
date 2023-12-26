package engine;

import java.util.ArrayList;

import model.ClientRequest;
import model.DatasetProfile;
import model.ServerRequest;
import rowchecks.IRowCheck;
import rowchecks.RowCheckFactory;
import utils.settings.DomainTypeSettings;
import utils.settings.DomainValueSettings;
import utils.settings.ForeignKeySettings;
import utils.settings.FormatSettings;
import utils.settings.NotNullSettings;
import utils.settings.NumberConstraintSettings;
import utils.settings.PrimaryKeySettings;

public class ClientToServerRequestTranslator {

	public ServerRequest createServerRequest(ClientRequest clientReq, ArrayList<DatasetProfile> profiles)
	{
		ServerRequest serverReq = new ServerRequest(clientReq.getViolationPolicy());
		serverReq.setRowChecks(getRowChecksFromOrder(clientReq, profiles));
		return serverReq;
	}

	private ArrayList<IRowCheck> getRowChecksFromOrder(ClientRequest order, ArrayList<DatasetProfile> profiles)
	{
		ArrayList<IRowCheck> checks = new ArrayList<IRowCheck>();
		RowCheckFactory factory = new RowCheckFactory(profiles);

		for (PrimaryKeySettings pkSettings : order.getPrimaryKeyChecks())
		{
			checks.add(factory.createPrimaryKeyCheck(pkSettings));
		}

		for (ForeignKeySettings fkSettings : order.getForeignKeyChecks())
		{		
			checks.add(factory.createBTreeForeignKeyCheck(fkSettings));
		}

		for (FormatSettings fSettings : order.getFormatChecks())
		{
			checks.add(factory.createFormatCheck(fSettings));
		}

		for (DomainTypeSettings dtSettings : order.getDomainTypeChecks())
		{
			checks.add(factory.createDomainTypeCheck(dtSettings));
		}
		
		for (DomainValueSettings dvSettings : order.getDomainValueChecks())
		{
			checks.add(factory.createDomainValuesCheck(dvSettings));
		}

		for (NotNullSettings nnSettings : order.getNotNullChecks())
		{
			checks.add(factory.createNotNullCheck(nnSettings));
		}

		for (NumberConstraintSettings ncSettings : order.getNumberConstraintChecks())
		{
			checks.add(factory.createNumericConstraintCheck(ncSettings));
		}

		return checks;
	}
}
