import styles from "./table_controls.module.scss";
import { Save20Regular as SaveIcon } from "@fluentui/react-icons";
import Button, { buttonVariants } from "../../buttons/Button";
import { useAppSelector } from "../../../store/hooks";
import { selectCurrentView, selectedViewIndex } from "../../forwards/forwardsSlice";
import { useUpdateTableViewMutation, useCreateTableViewMutation } from "apiSlice";
import ViewsPopover from "../../forwards/views/ViewsPopover";

function TableControls() {
  const currentView = useAppSelector(selectCurrentView);
  const currentViewIndex = useAppSelector(selectedViewIndex);
  const [updateTableView] = useUpdateTableViewMutation();
  const [createTableView] = useCreateTableViewMutation();
  const saveView = () => {
    let viewMod = { ...currentView };
    viewMod.saved = true;
    if (currentView.id === undefined || null) {
      createTableView({ view: viewMod, index: currentViewIndex });
      return;
    }
    updateTableView(viewMod);
  };
  return (
    <div className={styles.tableControls}>
      <div className={styles.leftContainer}>
        <div className={styles.upperContainer}>
          <ViewsPopover />
          {!currentView.saved && (
            <Button
              variant={buttonVariants.ghost}
              icon={<SaveIcon />}
              text={"Save"}
              onClick={saveView}
              className={"collapse-tablet danger"}
            />
          )}
        </div>
        <div className={styles.lowerContainer}></div>
      </div>
      <div className={styles.rightContainer}></div>
    </div>
  );
}

export default TableControls;
