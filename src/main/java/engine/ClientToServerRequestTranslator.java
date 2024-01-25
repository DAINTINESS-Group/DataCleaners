package engine;

import java.util.ArrayList;

import engine.clientRequest.ClientRequest;
import model.DatasetProfile;
import model.ServerRequest;
import rowchecks.api.IRowCheck;
import rowchecks.factory.RowCheckFactory;
import utils.settings.DomainTypeSettings;
import utils.settings.DomainValueSettings;
import utils.settings.ForeignKeySettings;
import utils.settings.FormatSettings;
import utils.settings.NotNullSettings;
import utils.settings.NumberConstraintSettings;
import utils.settings.PrimaryKeySettings;
import utils.settings.UserDefinedGroupSettings;
import utils.settings.UserDefinedHolisticSettings;
import utils.settings.UserDefinedRowSettings;

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

		for (UserDefinedRowSettings udrSettings : order.getUserDefinedRowSettings())
		{
			checks.add(factory.createUserDefinedCheck(udrSettings));
		}

		for (UserDefinedGroupSettings udgSettings : order.getUserDefinedGroupSettings())
		{
			checks.add(factory.createUserDefinedGroupCheck(udgSettings));
		}

		for (UserDefinedHolisticSettings udhSettings : order.getUserDefinedHolisticSettings())
		{
			checks.add(factory.createUserDefinedHolisticCheck(udhSettings, order.getTargetDataset()));
		}
		
		return checks;
	}
}
