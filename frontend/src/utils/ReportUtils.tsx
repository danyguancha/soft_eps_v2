
// utils/reportUtils.ts

export function debounce<T extends (...args: any[]) => any>(
  func: T,
  wait: number
): (...args: Parameters<T>) => void {
  let timeout: NodeJS.Timeout;
  return (...args: Parameters<T>) => {
    clearTimeout(timeout);
    timeout = setTimeout(() => func(...args), wait);
  };
}

/**
 * Función de filtrado personalizada para componentes de selección.
 * Determina si la opción dada coincide con la cadena de entrada comprobando
 * si dicha cadena está incluida en las propiedades `children` o `label` de la opción.
 *
 * @param input - La cadena de búsqueda introducida por el usuario.
 * @param option - El objeto de opción a filtrar, que puede contener las propiedades `children` o `label`.
 * @returns `true` si la opción coincide con la cadena de entrada; de lo contrario, `false`.
 */

export const createSelectFilterOption = (input: string, option: any): boolean => {

  const searchText = input.toLowerCase();
  
  if (option?.children) {
    return String(option.children).toLowerCase().includes(searchText);
  }
  if (option?.label) {
    return String(option.label).toLowerCase().includes(searchText);
  }
  return false;
};

export const getSelectPopupStyles = () => ({
  popup: {
    root: {
      maxHeight: '200px',
      overflowY: 'auto' as const
    }
  }
});
